# Person and vehicle regions that hold an animal

MegaDetector sorts every region into animal, person or vehicle. It gets some
animals wrong with high confidence: a Wood Duck drake, a Mandarin Duck, a
resting Canada Goose and a swimming grebe have all been labeled "person" or
"vehicle" at 0.4 to 0.9, with no animal region above the workspace's detector
confidence. Running the detector again on a tight crop of the region repeats
the label, so the detector cannot correct itself.

A photo whose only retained region is a person or vehicle is left out of
species classification on purpose, so a hiker is never named as a bird. Before
this check existed that also left the mislabeled animals with no subject: no
species, no mask, no quality analysis, and a burst of one bird split across
many encounters.

## What Vireo does

When classification runs, each photo with no retained animal region has its
strongest retained person or vehicle region shown to the classifier once. If
the classifier clearly recognizes a listed species there, the region is kept
as an animal subject and is classified, masked and scored like any other. If
not, the photo stays out of species classification as before.

"Clearly recognizes" means both of these, on the classifier's raw cosine
scores:

- the best species label scores at least 0.46, and
- it beats the best background prompt by at least 0.28.

This is stricter than the background gate used for ordinary animal regions,
because a wrong answer here makes a subject out of a car or a tree, while a
miss leaves the photo as it was. The thresholds were set on 280 hand-labeled
regions and checked on 245 more; `background_match.is_recognized_animal`
records the numbers. People and cars score 0.33 to 0.44 against every species
and are never kept.

The check needs cosine scores and background prompts, so it runs with BioCLIP
models and not with a timm classifier.

## What it misses

- Animals that match a plant prompt closely: flamingos (against "tamarisk")
  and small birds on a flowering branch.
- Animals not on the active species list. A frog is not kept under a list of
  birds, mammals and reptiles.
- A photo where a second, weaker person or vehicle region holds the animal.
  Only the strongest is examined.

For those, draw the subject box in the Subjects panel
(`docs/reviewer-selected-subjects.md`).

## Where it shows

- The classify step's summary counts the regions kept in that run
  ("3 person/vehicle regions kept as subjects").
- The lightbox Subjects panel and the pipeline inspector say when a subject is
  a region the detector labeled person or vehicle. Its detector percentage is
  the detector's confidence in that label, not in "animal".
- The Process plan counts regions not yet examined under the selected model
  and species list as classify work, so "Already classified" means none are
  waiting.

A kept region can be rejected like any other with **Not an animal**.

## Storage

`subject_candidates` holds one row per examined region: the detector's own
category, the classifier model and label set, the best species and its score,
the best background prompt and its score, and whether the region was kept. A
kept region's `detections.category` is `animal`. The answer is stored per
(model, label set), so changing the species list examines the remaining
regions again; a region already kept stays kept.

Detection ids are content-addressed on the detector's category. When the
detector writes the same region again the row keeps its `animal` category, and
when a new detector runtime reproduces the region exactly it stays a subject.
A region the new runtime moves is examined again.
