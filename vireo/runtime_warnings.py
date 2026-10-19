"""CPU-only runtime warnings for expensive ML jobs.

Job routes size their work up front and, when ONNX Runtime has no
accelerated provider, attach a dismissible warning to the job so the user
learns *before* a long run that it will be slow. Shared by the classify,
extract-masks, precompute-embeddings and pipeline launchers and by the
after-import chain; ``runtime_execution_info`` also feeds the system-info
and report-issue diagnostics.
"""

import logging

log = logging.getLogger(__name__)


_ACCELERATED_RUNTIME_PROVIDERS = {
    "ACLExecutionProvider",
    "ArmNNExecutionProvider",
    "CANNExecutionProvider",
    "CoreMLExecutionProvider",
    "CUDAExecutionProvider",
    "DmlExecutionProvider",
    "MIGraphXExecutionProvider",
    "NNAPIExecutionProvider",
    "OpenVINOExecutionProvider",
    "QNNExecutionProvider",
    "ROCMExecutionProvider",
    "TensorrtExecutionProvider",
    "VitisAIExecutionProvider",
    "WebGPUExecutionProvider",
}
_PROVIDER_DEVICE_INFO = {
    "ACLExecutionProvider": ("ACL", "Arm Compute Library acceleration"),
    "ArmNNExecutionProvider": ("ArmNN", "Arm NN acceleration"),
    "CANNExecutionProvider": ("CANN", "Huawei CANN acceleration"),
    "CoreMLExecutionProvider": ("CoreML", "Apple CoreML acceleration"),
    "CUDAExecutionProvider": ("CUDA", "NVIDIA CUDA acceleration"),
    "DmlExecutionProvider": ("DirectML", "Microsoft DirectML acceleration"),
    "MIGraphXExecutionProvider": ("MIGraphX", "AMD MIGraphX acceleration"),
    "NNAPIExecutionProvider": ("NNAPI", "Android NNAPI acceleration"),
    "OpenVINOExecutionProvider": ("OpenVINO", "Intel OpenVINO acceleration"),
    "QNNExecutionProvider": ("QNN", "Qualcomm QNN acceleration"),
    "ROCMExecutionProvider": ("ROCm", "AMD ROCm acceleration"),
    "TensorrtExecutionProvider": ("TensorRT", "NVIDIA TensorRT acceleration"),
    "VitisAIExecutionProvider": ("Vitis AI", "AMD/Xilinx Vitis AI acceleration"),
    "WebGPUExecutionProvider": ("WebGPU", "WebGPU acceleration"),
}
_CPU_WARNING_MIN_WORK_ITEMS = 25

def runtime_execution_info():
    """Return ONNX Runtime provider/device information for UI diagnostics."""
    import platform

    info = {
        "platform": platform.platform(),
        "device": "CPU",
        "device_detail": "No GPU acceleration",
        "onnxruntime_version": None,
        "onnxruntime_providers": [],
        "accelerated_provider_available": False,
        "gpu_provider_available": False,
        "cpu_only": True,
    }
    try:
        import onnxruntime as ort

        info["onnxruntime_version"] = ort.__version__
        available = list(ort.get_available_providers())
        info["onnxruntime_providers"] = available
        accelerated = [
            p for p in available if p in _ACCELERATED_RUNTIME_PROVIDERS
        ]
        info["accelerated_provider_available"] = bool(accelerated)
        # Back-compat for callers that already key off this field.
        info["gpu_provider_available"] = bool(accelerated)
        info["cpu_only"] = not bool(accelerated)

        if accelerated:
            device, detail = _PROVIDER_DEVICE_INFO.get(
                accelerated[0],
                (accelerated[0].replace("ExecutionProvider", ""), "Hardware acceleration"),
            )
            info["device"] = device
            info["device_detail"] = detail
        else:
            info["device"] = "CPU"
            info["device_detail"] = "GPU not available - using CPU"
    except ImportError:
        info["device_detail"] = "onnxruntime not installed"

    return info

def build_cpu_runtime_warning(job_type, *, work_units=None, reason=None):
    """Build a dismissible CPU-only warning for expensive ML jobs."""
    if work_units is None or work_units < _CPU_WARNING_MIN_WORK_ITEMS:
        return None

    info = runtime_execution_info()
    if info["accelerated_provider_available"]:
        return None

    providers = info["onnxruntime_providers"]
    provider_text = ", ".join(providers) if providers else "none detected"
    label = job_type.replace("-", " ").replace("_", " ").title()
    warning = {
        "id": "cpu-only-ml",
        "kind": "cpu-only-ml",
        "title": "Using CPU only",
        "message": f"This {label} job may be much slower than expected.",
        "detail": f"Available ONNX Runtime providers: {provider_text}",
        "next_action": (
            "Install the CUDA/CoreML runtime, check accelerator "
            "availability, or continue on CPU."
        ),
        "device": info["device"],
        "device_detail": info["device_detail"],
        "onnxruntime_version": info["onnxruntime_version"],
        "onnxruntime_providers": providers,
        "work_units": work_units,
        "reason": reason or "large_ml_job_cpu_only",
    }
    log.warning(
        "CPU-only runtime warning for %s job (%s work item%s): "
        "providers=%s device=%s reason=%s",
        job_type,
        work_units,
        "" if work_units == 1 else "s",
        provider_text,
        info["device"],
        warning["reason"],
    )
    return warning


def runtime_warning_work_units(description, count_fn):
    """Best-effort sizing for warnings; never block job submission."""
    try:
        return count_fn()
    except Exception:  # broad: a sizing query must not break the warning; the count stays unknown
        log.debug("Could not size %s for runtime warning", description, exc_info=True)
        return None
