//! Experimental filters only: not linked into the shipped application.
//! Copy borrowed NumPy data before detaching from Python, and transfer owned
//! output buffers back without another copy. All copies count in benchmarks.

use numpy::ndarray::{ArrayD, IxDyn};
use numpy::{IntoPyArray, PyArrayDyn, PyReadonlyArray1, PyReadonlyArrayDyn};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rayon::prelude::*;

const MAX_PIXELS: usize = 8_000_000;

fn bad(message: &str) -> PyErr {
    PyValueError::new_err(message.to_owned())
}

// NumPy pad(mode="reflect") / OpenCV BORDER_REFLECT_101 / SciPy "mirror".
fn reflect(index: isize, length: usize) -> usize {
    if length == 1 {
        return 0;
    }
    let period = 2 * (length as isize - 1);
    let folded = index.rem_euclid(period);
    if folded < length as isize {
        folded as usize
    } else {
        (period - folded) as usize
    }
}

fn snapshot(input: PyReadonlyArrayDyn<'_, f32>) -> PyResult<(Vec<f32>, Vec<usize>)> {
    let view = input.as_array();
    let shape = view.shape().to_vec();
    if !(2..=3).contains(&shape.len()) || shape.contains(&0) {
        return Err(bad("Expected a nonempty H×W or H×W×C float32 array"));
    }
    if shape[0]
        .checked_mul(shape[1])
        .is_none_or(|n| n > MAX_PIXELS)
        || (shape.len() == 3 && shape[2] > 4)
    {
        return Err(bad("Input exceeds the experiment's tile limit"));
    }
    let pixels: Vec<_> = view.iter().copied().collect();
    if pixels.iter().any(|v| !v.is_finite()) {
        return Err(bad("Input must contain finite values"));
    }
    Ok((pixels, shape))
}

fn result<'py>(
    py: Python<'py>,
    data: Vec<f32>,
    shape: &[usize],
) -> PyResult<Bound<'py, PyArrayDyn<f32>>> {
    ArrayD::from_shape_vec(IxDyn(shape), data)
        .map(|a| a.into_pyarray(py))
        .map_err(|_| bad("Invalid result shape"))
}

#[pyclass]
struct Filters {
    pool: rayon::ThreadPool,
}

#[pymethods]
impl Filters {
    #[new]
    fn new(threads: usize) -> PyResult<Self> {
        if !(1..=4).contains(&threads) {
            return Err(bad("Use between one and four worker threads"));
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .map_err(|e| bad(&format!("Could not create filter pool: {e}")))?;
        Ok(Self { pool })
    }

    fn gaussian<'py>(
        &self,
        py: Python<'py>,
        input: PyReadonlyArrayDyn<'py, f32>,
        kernel: PyReadonlyArray1<'py, f32>,
    ) -> PyResult<Bound<'py, PyArrayDyn<f32>>> {
        let (data, shape) = snapshot(input)?;
        let weights: Vec<_> = kernel.as_array().iter().copied().collect();
        if weights.is_empty()
            || weights.len() % 2 == 0
            || weights.len() > 129
            || weights.iter().any(|v| !v.is_finite())
        {
            return Err(bad(
                "Kernel must have an odd length of at most 129 finite weights",
            ));
        }
        let height = shape[0];
        let width = shape[1];
        let channels = shape.get(2).copied().unwrap_or(1);
        let row_len = width * channels;
        let radius = (weights.len() / 2) as isize;
        let filtered = py.detach(|| {
            self.pool.install(|| {
                let mut vertical = vec![0.0_f32; data.len()];
                vertical
                    .par_chunks_mut(row_len)
                    .enumerate()
                    .for_each(|(y, row)| {
                        for (tap, weight) in weights.iter().enumerate() {
                            let sy = reflect(y as isize + tap as isize - radius, height);
                            let source = &data[sy * row_len..(sy + 1) * row_len];
                            for (out, value) in row.iter_mut().zip(source) {
                                *out += value * weight;
                            }
                        }
                    });
                let mut output = vec![0.0_f32; data.len()];
                output
                    .par_chunks_mut(row_len)
                    .enumerate()
                    .for_each(|(y, row)| {
                        for (tap, weight) in weights.iter().enumerate() {
                            for x in 0..width {
                                let sx = reflect(x as isize + tap as isize - radius, width);
                                for c in 0..channels {
                                    row[x * channels + c] +=
                                        vertical[y * row_len + sx * channels + c] * weight;
                                }
                            }
                        }
                    });
                output
            })
        });
        result(py, filtered, &shape)
    }

    fn bilateral<'py>(
        &self,
        py: Python<'py>,
        input: PyReadonlyArrayDyn<'py, f32>,
        sigma_spatial: f64,
        sigma_range: f64,
        radius: usize,
    ) -> PyResult<Bound<'py, PyArrayDyn<f32>>> {
        if !(sigma_spatial.is_finite()
            && sigma_spatial > 0.0
            && sigma_range.is_finite()
            && sigma_range > 0.0)
            || radius > 8
        {
            return Err(bad(
                "Expected finite positive sigmas and radius at most eight",
            ));
        }
        let (data, shape) = snapshot(input)?;
        if shape.len() != 2 {
            return Err(bad(
                "Bilateral filter requires a two-dimensional luma plane",
            ));
        }
        let inv_spatial = (1.0 / (2.0 * sigma_spatial * sigma_spatial)) as f32;
        let inv_range = (1.0 / (2.0 * sigma_range * sigma_range)) as f32;
        if !inv_spatial.is_finite() || !inv_range.is_finite() {
            return Err(bad("Sigma is too small for float32 arithmetic"));
        }
        let height = shape[0];
        let width = shape[1];
        let radius = radius as isize;
        let offsets: Vec<_> = (-radius..=radius)
            .flat_map(|dy| {
                (-radius..=radius)
                    .map(move |dx| (dy, dx, -((dy * dy + dx * dx) as f32) * inv_spatial))
            })
            .collect();
        let filtered = py.detach(|| {
            self.pool.install(|| {
                let mut output = vec![0.0_f32; data.len()];
                output
                    .par_chunks_mut(width)
                    .enumerate()
                    .for_each(|(y, row)| {
                        for (x, out) in row.iter_mut().enumerate() {
                            let center = data[y * width + x];
                            let mut sum = 0.0_f32;
                            let mut total = 0.0_f32;
                            for &(dy, dx, spatial) in &offsets {
                                let sy = reflect(y as isize + dy, height);
                                let sx = reflect(x as isize + dx, width);
                                let sample = data[sy * width + sx];
                                let diff = sample - center;
                                let weight = (spatial - diff * diff * inv_range).exp();
                                sum += sample * weight;
                                total += weight;
                            }
                            *out = sum / total;
                        }
                    });
                output
            })
        });
        result(py, filtered, &shape)
    }
}

#[pymodule]
fn vireo_detail_experiment(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Filters>()
}
