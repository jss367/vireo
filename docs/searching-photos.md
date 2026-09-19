# Searching photos

The shared search box on photo pages searches metadata already scanned into
your library: filenames, folder paths and names, keywords, species names,
camera and lens details, dates, numeric camera settings, ratings, color labels,
locations, and current predictions. It also searches the values of stored file
tags, including captions, titles, authors, copyright, and custom tags.

Words can match different fields. Matches are substrings, so `haw` finds
`hawk`. Use uppercase operators to combine searches:

| Search | Finds photos with |
| --- | --- |
| `hawk Canon` | Both words anywhere in their metadata |
| `hawk AND Canon` | The same matches as `hawk Canon` |
| `hawk OR owl` | Either word |
| `hawk NOT perched` | `hawk` and no `perched` match |
| `(hawk OR owl) AND Monterey` | Either bird word, plus `Monterey` |
| `"morning light"` | That exact phrase within one metadata value |
| `NOT (hawk OR owl)` | Neither word |

`NOT` applies first, then `AND`, then `OR`. Parentheses override that order.
Lowercase `and`, `or`, and `not` are ordinary search words. Put uppercase
operator words or filenames containing parentheses in double quotes to search
for them literally. Within a phrase, `\"` means a literal double quote and
`\\` means a literal backslash. Percent signs and underscores are literal.

Results update as you type. An unfinished expression, such as `hawk OR`,
shows a message and keeps the last valid filters applied until you finish or
clear the search. Searches are limited to 4,096 characters and 16 levels of
parentheses or negation.

Search combines with the page's scope and other filters. Save it with
**Filters → Save as Collection…** to reuse the whole expression. In the rule
builder, **All metadata** offers literal **contains** and **doesn't contain**
rules; Boolean syntax belongs in the main search box.

Metadata edits take effect immediately. For keywords, ratings, and color
labels, search uses the editable library values so removed tags and old
ratings in cached file metadata do not keep matching. Prediction searches
use the current label set and visible detections, including review status.
