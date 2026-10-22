# Public labels for wildlife keeper evidence

Assessment date: 2026-10-21. No dataset archives or new model weights were
downloaded for this assessment. Public independent annotations are a potential
route to training and validation; they should not be dismissed merely because
the local photographer has not supplied labels. The target still needs to match
the evidence: selected-region animal validity and obvious severe defocus are
separate from generic quality, pose, identity or whole-image species labels.

| Public evidence | Independently labeled information | Useful role and remaining limit |
| --- | --- | --- |
| [Chinese University of Hong Kong blur detection dataset](https://www.cse.cuhk.edu.hk/leojia/projects/dblurdetect/dataset.html) | 1,000 natural photographs with human blur-region masks, produced by ten annotators; includes defocus and partial motion blur. | A substantive source of genuine blur labels for a focus-cue pilot. The landing page supplies no matching animal-region labels or explicit redistribution license. Animal-subject focus validity cannot be inferred from a blurred background. Establish the image terms and whether a relevant animal subset has independent semantic annotations before transferring results. |
| [Oxford animal pet images](https://robots.ox.ac.uk/~vgg/data/pets/) | Breed/species labels, head boxes and foreground/background trimaps across 37 pet categories; the official page declares attribution/share-alike licensing. | Human animal-region support, with pose/scale/lighting variation. It does not label natural severe defocus or establish a wildlife/native-camera domain. A missing animal box is not proof that another crop contains no animal. Follow image attribution and share-alike terms when using derived data. |
| [PaQ-2-PiQ picture and patch quality study](https://baidut.github.io/PaQ-2-PiQ/) | Around 40,000 pictures, 120,000 patches and four million human quality judgments. | Real local perceptual-quality supervision is stronger than synthetic blur alone. Patch quality is not a semantic animal annotation or a pure focus label. Match the label definition, source-image split and license before any bounded pilot; do not turn a low quality score into a severe-defocus label. |
| [Grevy’s zebra census photographs](https://huggingface.co/datasets/imageomics/GZCD) | Wildlife boxes and region annotations; `manual` distinguishes human-reviewed annotations. The dataset card declares attribution licensing. | Potential animal-region support after filtering actual human verification. The card explicitly defines `CA_score` as a classifier output about identifiability/viewpoint; it is **not independent human blur ground truth**. Neither that score nor census inclusion should silently label photographic usability. |
| [Open Images official downloads](https://storage.googleapis.com/openimages/web/download.html) and [hosted image details](https://github.com/cvdfoundation/open-images-dataset) | Human image/region annotations; source/annotation and image licensing information are published separately. | Candidate animal/background support after checking annotation coverage and record-level image terms. Hosted copies have a maximum long edge of 1,024 pixels. These copies cannot establish native sensor/embedded-raster focus thresholds for the private Z8 photographs. Incomplete annotations cannot justify automatic no-animal labels. |
| [Macaulay Library technical photo ratings](https://ebird.freshdesk.com/en/support/solutions/articles/48001064392-rating-media) | Human technical photo ratings explicitly consider primary-subject focus, visibility, exposure, resolution and other factors. | Relevant bird-photo supervision, but aggregate stars conflate several causes and do not bind the app's selected crop. A low star rating is not a pure blur label, and image reuse rights/access must be established separately. A matched region/target study could be useful; do not scrape a large image corpus on that assumption. |

These sources can reduce the amount of new annotation needed. None of the pages
assessed establishes a ready-made matched set of independently verified animal
regions **and** genuine severe-defocus labels in the private native-raster
recipe. This is a limitation of the inspected candidates, not a claim that no
such public dataset exists. The separate-gate candidate in this package can
use compatible labels once their provenance and target mapping are established.

A bounded next experiment should first inspect annotation metadata and terms,
then select roughly 20 source-disjoint cases from an explicitly licensed source:
clear animal regions, naturally defocused animal regions, verified non-animal
regions and ambiguous controls. Keep source/scene relatives together; preserve
original human labels, annotator provenance, downloaded-byte hashes, resolution,
and source license/attribution. Do not manufacture binary labels by thresholding
a model's quality score. Request budget only for that named slice, not an entire
archive. If only separate animal and generic-blur corpora are available, report
them as separate gate evidence, then test transfer on matched wildlife regions.

The private first tranche remains a useful domain diagnostic and annotation
artifact: 18 native regions from disjoint saved encounters, with seven
development cases, six held-out cases and five already-seen diagnostic cases.
There are currently **zero independent region labels** in that bundle. It is
one available Nikon Z8 shoot; other sampled catalog originals are offline. A
single-photographer blinded protocol is recorded, with optional additional
raters and disagreement routed to uncertainty. Neither prior flags nor feature
extraction makes these cases labeled ground truth.

A real scoped producer still needs an agreed narrow target and error budget,
compatible independently labeled development negatives/positives for both gates,
frozen model/thresholds, and held-out outcomes with uncertainty and error bounds.
A one-shoot study can support only its declared camera/source/recipe conditions;
it does not require a universal multi-camera benchmark to be useful. Successful
public pretraining is not a substitute for measuring the claimed deployment
scope. Runtime integration should consume reviewed, versioned evidence from the
actual producer and abstain outside it, preserving explicit photographer choices.
