# Vireo — Core Philosophy

A transparent AI-powered organizer for wildlife photos that respects your filesystem and never hides what it's doing.

## XMP is truth, the database is a cache

Your photos and their metadata live on your filesystem, not locked in our app. The SQLite database is a fast index that can be rebuilt at any time from your XMP sidecars. Vireo is a lens on top of your files, not a vault around them.

## Show the user what's happening

No black boxes. If it's scanning, you see the file count ticking up. If the model is downloading, you see the progress bar. If classification is running, you see which photo it's on and what phase of the pipeline you're in. If something fails, you see the traceback. The log panel, job progress, pipeline inspector, and audit system all exist because we'd rather be transparent than polished.

## Work with the ecosystem, don't replace it

XMP stores metadata. The NAS holds files. iNaturalist provides taxonomy. BioCLIP classifies species. Vireo orchestrates and organizes — it doesn't try to own the whole pipeline. Import from Lightroom, sync to XMP, browse on any device.

## Design for scale

Design to be able to handle tens of millions of images.

## Responsiveness over speed

A task can take a long time, but the app should never take a long time to respond. Long operations run in background jobs with progress streaming. The UI stays interactive. Every button gives immediate feedback.

## Let the user control the pipeline

## Respect the photographer's judgment

AI suggests, humans decide.
