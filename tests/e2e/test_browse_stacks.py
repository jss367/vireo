import re

from playwright.sync_api import expect

from e2e.stack_seed import seed_browse_stack


def test_browse_stacks_collapse_expand_and_select(live_server, page):
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )
    db.save_detections(burst_ids[2], [{
        "box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
        "confidence": 0.91,
        "category": "animal",
    }], detector_model="test-detector")

    page.goto(f"{live_server['url']}/browse")
    cards = page.locator("#grid > .grid-card")
    expect(cards).to_have_count(5)

    page.locator("#browseStacksToggle").check()
    expect(cards).to_have_count(3)
    expect(page.locator("#filterSummary")).to_contain_text("3 items · 5 photos")
    assert page.evaluate(
        """() => browseStackCoverCompare(
          {id: 1, rating: 0, width: 1, height: 1},
          {id: 2, rating: null, width: 999, height: 999}
        ) < 0"""
    )

    stack_card = page.locator(
        f'.grid-card[data-id="{burst_ids[1]}"]'
    )
    expect(stack_card).to_be_visible()
    badge = stack_card.locator(".browse-stack-badge")
    expect(badge).to_have_text("▦3")
    badge.click()

    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    expect(tray).to_be_visible()
    expect(tray.locator(".browse-stack-member")).to_have_count(3)
    expect(tray).to_contain_text("Burst")
    expect(tray).to_contain_text("hawk1.jpg")
    expect(tray).to_contain_text("hawk2.jpg")
    expect(tray).to_contain_text("hawk3.jpg")
    page.locator("#detBoxToggle").click()
    expect(tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[2]}"] .det-box'
    )).to_be_visible()
    page.evaluate(
        """photoId => {
          window._testOriginalOrientationCheck = window.vireoPhotoHasOrientationEdit;
          window.vireoPhotoHasOrientationEdit = function(id) { return id === photoId; };
          document.dispatchEvent(new CustomEvent('lightbox:renderchanged', {
            detail: {photoIds: [photoId]},
          }));
        }""",
        burst_ids[2],
    )
    expect(tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[2]}"] .det-box'
    )).to_be_hidden()
    page.evaluate(
        """photoId => {
          window.vireoPhotoHasOrientationEdit = window._testOriginalOrientationCheck;
          delete window._testOriginalOrientationCheck;
          document.dispatchEvent(new CustomEvent('lightbox:renderchanged', {
            detail: {photoIds: [photoId]},
          }));
        }""",
        burst_ids[2],
    )
    expect(tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[2]}"] .det-box'
    )).to_be_visible()
    page.locator("#detBoxToggle").click()
    expect(tray.locator(".det-box")).to_have_count(0)
    assert page.evaluate(
        """coverId => {
          var cover = photos.find(function(photo) { return photo.id === coverId; });
          var member = browseStackMembers[String(coverId)].find(function(photo) {
            return photo.id === coverId;
          });
          return cover === member;
        }""",
        burst_ids[1],
    )

    badge.click()
    expect(tray).to_be_hidden()
    assert page.evaluate(
        "coverId => browsePhotoNavigationList(coverId) === photos",
        burst_ids[1],
    )
    badge.click()
    expect(tray).to_be_visible()

    cover_member = tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[1]}"]'
    )
    cover_member.dblclick()
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")
    page.locator("[title='Next (→)']").click()
    expect(page.locator("#lightboxFilename")).to_have_text("hawk3.jpg")
    page.keyboard.press("Escape")
    page.wait_for_function(
        "photoId => selectedPhotoId === photoId",
        arg=burst_ids[2],
    )
    expect(page.locator("#detailFilename")).to_have_text("hawk3.jpg")

    hidden_member = tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[2]}"]'
    )
    hidden_member.click()
    expect(hidden_member).to_have_class("browse-stack-member selected")
    expect(page.locator("#batchCount")).to_have_text("1 selected")
    expect(page.locator("#detailFilename")).to_have_text("hawk3.jpg")

    other_hidden_member = tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[0]}"]'
    )
    other_hidden_member.click(button="right")
    page.wait_for_function(
        "photoId => selectedPhotoId === photoId",
        arg=burst_ids[0],
    )
    assert page.evaluate(
        "coverId => selectedIndex === photos.findIndex(function(photo) { return photo.id === coverId; })",
        burst_ids[1],
    )
    page.evaluate("() => closeContextMenu()")
    hidden_member.click()
    expect(page.locator("#detailFilename")).to_have_text("hawk3.jpg")

    page.evaluate(
        """coverId => {
          document.querySelector(
            '.browse-stack-tray[data-stack-cover-id="' + coverId + '"]'
          ).dataset.beforeColorRefresh = '1';
        }""",
        burst_ids[1],
    )
    page.evaluate("() => setColorLabel('red')")
    page.wait_for_function(
        "photoId => colorLabels[photoId] === 'red'",
        arg=burst_ids[2],
    )
    expect(tray).not_to_have_attribute("data-before-color-refresh", "1")

    page.locator("#batchBar button", has_text="Export").click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    expect(page.locator("#exportPreview")).to_contain_text("hawk3")
    page.locator("#exportOverlay button", has_text="Cancel").click()

    other_card_id = live_server["data"]["photos"][3]
    page.locator(f'.grid-card[data-id="{other_card_id}"]').click(
        modifiers=["Meta"]
    )
    tray.get_by_role("button", name="Collapse stack").click()
    multi_collapse_state = page.evaluate(
        """() => {
          var preview = getBrowseShortcutPhoto();
          return {
            selectedPhotoId: selectedPhotoId,
            selectedIds: Array.from(selectedPhotos),
            previewId: preview && preview.photo.id,
            topLevelNavigation: !!preview && preview.navigationPhotos === photos,
          };
        }"""
    )
    assert multi_collapse_state == {
        "selectedPhotoId": burst_ids[2],
        "selectedIds": [burst_ids[2], other_card_id],
        "previewId": burst_ids[1],
        "topLevelNavigation": True,
    }
    badge.click()
    expect(tray).to_be_visible()
    hidden_member.click()

    tray.get_by_role("button", name="Collapse stack").click()
    expect(tray).to_be_hidden()
    page.wait_for_function(
        "coverId => selectedPhotoId === coverId",
        arg=burst_ids[1],
    )
    expect(page.locator("#detailFilename")).to_have_text("hawk2.jpg")
    assert page.evaluate(
        "coverId => browsePhotoNavigationList(coverId) === photos",
        burst_ids[1],
    )
    badge.click()
    expect(tray).to_be_visible()

    tray.get_by_role("button", name="Select all").click()
    expect(page.locator("#batchCount")).to_have_text("3 selected \u00b7 1 stack")
    for photo_id in burst_ids:
        expect(
            tray.locator(f'.browse-stack-member[data-id="{photo_id}"]')
        ).to_have_class("browse-stack-member selected")

    tray.get_by_role("button", name="Collapse stack").click()
    expect(tray).to_be_hidden()
    assert page.evaluate(
        """coverId => {
          var preview = getBrowseShortcutPhoto();
          return selectedPhotos.size === 3 && selectedPhotoId === coverId
            && preview.photo.id === coverId && preview.navigationPhotos === photos;
        }""",
        burst_ids[1],
    )
    badge.click()
    expect(tray).to_be_visible()

    page.evaluate("() => setSelectionWildlifeExcluded(true)")
    page.wait_for_function(
        """ids => ids.every(function(id) {
          var photo = findBrowsePhoto(id);
          return photo && photo.wildlife_excluded;
        })""",
        arg=burst_ids,
    )
    expect(tray.locator(".no-wildlife-badge")).to_have_count(3)
    page.evaluate("() => setSelectionWildlifeExcluded(false)")
    expect(tray.locator(".no-wildlife-badge")).to_have_count(0)

    page.evaluate(
        """photoId => document.dispatchEvent(new CustomEvent('lifelist:changed', {
          detail: {species: 'Test species', photoId: photoId},
        }))""",
        burst_ids[0],
    )
    expect(tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[0]}"] .representative-badge'
    )).to_be_visible()
    page.evaluate(
        """photoId => document.dispatchEvent(new CustomEvent('lifelist:changed', {
          detail: {species: 'Test species', photoId: photoId},
        }))""",
        burst_ids[2],
    )
    expect(tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[0]}"] .representative-badge'
    )).to_have_count(0)
    expect(tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[2]}"] .representative-badge'
    )).to_be_visible()

    page.locator("#batchBar button", has_text="★5").click()
    page.wait_for_function(
        """ids => ids.every(function(id) {
          var photo = findBrowsePhoto(id);
          return photo && photo.rating === 5;
        })""",
        arg=burst_ids,
    )
    expect(page.locator("#ratingMixed")).to_be_hidden()
    page.locator("#batchBar button", has_text="Flag").click()
    page.wait_for_function(
        """ids => ids.every(function(id) {
          var photo = findBrowsePhoto(id);
          return photo && photo.flag === 'flagged';
        })""",
        arg=burst_ids,
    )
    expect(page.locator("#flagMixed")).to_be_hidden()

    page.evaluate(
        """ids => {
          photos.find(function(photo) { return photo.browse_stack; })._similarity = 0.95;
          bestBatchData = {
            best_photo_id: ids[0],
            suggested_reject_ids: ids.slice(1),
          };
          return applyBestBatchPickAndReject();
        }""",
        burst_ids,
    )
    page.wait_for_function(
        """ids => ids.every(function(id, index) {
          var photo = findBrowsePhoto(id);
          return photo && photo.flag === (index === 0 ? 'flagged' : 'rejected');
        })""",
        arg=burst_ids,
    )
    expect(page.locator("#flagMixed")).to_be_visible()
    new_cover = page.locator(f'.grid-card[data-id="{burst_ids[0]}"]')
    expect(new_cover).to_be_visible()
    expect(new_cover.locator(".browse-stack-badge")).to_have_text("▦3")
    expect(page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[0]}"]'
    )).to_be_visible()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_have_count(0)
    assert page.evaluate(
        "coverId => photos.find(function(photo) { return photo.id === coverId; })._similarity",
        burst_ids[0],
    ) == 0.95

    # Stacks is a presentation preference, so it survives a return to Browse.
    page.reload()
    expect(page.locator("#browseStacksToggle")).to_be_checked()
    expect(page.locator("#grid > .grid-card")).to_have_count(3)

    # A collapsed stack has no hydrated member cache. Rejecting its current
    # cover still hydrates that one group and promotes the correct replacement.
    assert page.evaluate("() => Object.keys(browseStackMembers).length") == 0
    # A collapsed stack card stands for its whole stack, so one click selects
    # every frame behind it and the panel opens as the batch inspector. No
    # member cache is needed: the cover carries its member ids.
    page.locator(f'.grid-card[data-id="{burst_ids[0]}"]').click()
    page.wait_for_function(
        """ids => selectedPhotoId === null && selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        arg=burst_ids,
    )
    expect(page.locator("#selectionCount")).to_have_text(
        "3 photos selected \u00b7 1 stack"
    )
    page.evaluate(
        "photoId => setFlagFor(photoId, 'rejected')",
        burst_ids[0],
    )
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[0]}"]')).to_have_count(0)
    # Promoting a new cover moves no photo in or out of the stack, so the
    # selection the click made survives it intact.
    page.wait_for_function(
        """ids => selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        arg=burst_ids,
    )
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_have_class(
        "grid-card has-browse-stack selected"
    )

    # Restore the original cover, then demote it while it remains part of a
    # batch. Exact selected IDs stay unchanged while preview maps the now-hidden
    # focused member to the collapsed replacement cover.
    page.evaluate(
        "photoId => setFlagFor(photoId, 'flagged')",
        burst_ids[0],
    )
    expect(page.locator(f'.grid-card[data-id="{burst_ids[0]}"]')).to_be_visible()
    page.evaluate(
        """ids => {
          selectedPhotoId = ids[0];
          selectedIndex = photos.findIndex(function(photo) { return photo.id === ids[0]; });
          selectedPhotos = new Set([ids[0], ids[1]]);
        }""",
        [burst_ids[0], live_server["data"]["photos"][3]],
    )
    page.evaluate(
        "photoId => setFlagFor(photoId, 'rejected')",
        burst_ids[0],
    )
    assert page.evaluate(
        """ids => {
          var active = getActiveSelection();
          var preview = getBrowseShortcutPhoto();
          return selectedPhotoId === ids[0]
            && active.length === 2 && active.includes(ids[0]) && active.includes(ids[1])
            && preview.photo.id !== ids[0] && preview.navigationPhotos === photos;
        }""",
        [burst_ids[0], live_server["data"]["photos"][3]],
    )

    assert page.evaluate(
        """async coverId => {
          var originalSafeFetch = safeFetch;
          var memberLoads = 0;
          delete browseStackMembers[String(coverId)];
          browseStackErrors[String(coverId)] = 'Could not load this stack.';
          expandedBrowseStacks.delete(coverId);
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids') memberLoads++;
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            await toggleBrowseStack(null, coverId);
          } finally {
            safeFetch = originalSafeFetch;
          }
          return memberLoads === 1
            && !!browseStackMembers[String(coverId)]
            && !browseStackErrors[String(coverId)];
        }""",
        burst_ids[1],
    )

    hydration_chunks = page.evaluate(
        """async coverId => {
          var cover = photos.find(function(photo) { return photo.id === coverId; });
          cover.browse_stack.photo_ids = Array(501).fill(coverId);
          delete browseStackMembers[String(coverId)];
          var originalSafeFetch = safeFetch;
          var chunks = [];
          safeFetch = function(url, options) {
            if (url === '/api/photos/by-ids') {
              chunks.push(JSON.parse(options.body).photo_ids.length);
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            await reconcileBrowseStackCovers([coverId]);
          } finally {
            safeFetch = originalSafeFetch;
          }
          return chunks;
        }""",
        burst_ids[1],
    )
    assert hydration_chunks == [500, 1]

    assert page.evaluate(
        """async args => {
          var coverId = args.coverId;
          var oldCover = photos.find(function(photo) { return photo.id === coverId; });
          var oldIndex = photos.indexOf(oldCover);
          var stack = oldCover.browse_stack;
          stack.photo_ids = args.memberIds;
          delete browseStackMembers[String(coverId)];
          expandedBrowseStacks.delete(coverId);
          var releaseExpand;
          var originalSafeFetch = safeFetch;
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids') {
              return new Promise(function(resolve) { releaseExpand = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          var pendingExpand = toggleBrowseStack(null, coverId);
          await new Promise(function(resolve) { setTimeout(resolve, 0); });
          oldCover.browse_stack = null;
          var replacement = Object.assign({}, oldCover, {
            id: coverId + 100000,
            browse_stack: stack,
          });
          photos[oldIndex] = replacement;
          releaseExpand({photos: [oldCover]});
          await pendingExpand;
          safeFetch = originalSafeFetch;
          return browseStackMembers[String(coverId)] === undefined;
        }""",
        {"coverId": burst_ids[1], "memberIds": burst_ids},
    )


def test_clearing_filters_preserves_photo_that_becomes_hidden_stack_member(
    live_server, page
):
    """A widening clear expands a new stack rather than dropping its selected member."""
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    # A general keyword narrows the view to one member without splitting the
    # stack: only species and location keywords are part of a burst's identity.
    db.tag_photo(burst_ids[0], db.add_keyword("Portfolio", kw_type="general"))
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("VireoFilter.isReady()")
    page.locator("#browseStacksToggle").check()
    page.evaluate("updateThumbSize(400)")
    page.evaluate("VireoFilter.addRule('keyword', 'is', 'Portfolio')")
    page.wait_for_function("() => photos.length === 1")

    selected_id = burst_ids[0]
    selected = page.locator(f'.grid-card[data-id="{selected_id}"]')
    selected.click()
    top_before = selected.evaluate(
        """card => card.getBoundingClientRect().top -
          document.getElementById('gridContainer').getBoundingClientRect().top"""
    )

    page.click(".vf-clear")
    page.wait_for_function("id => selectedPhotoId === id", arg=selected_id)
    member = page.locator(f'.browse-stack-member[data-id="{selected_id}"]')
    member.wait_for(state="visible")
    page.wait_for_timeout(100)

    expect(member).to_have_class("browse-stack-member selected")
    assert page.evaluate("selectedPhotos.size") == 0
    assert page.evaluate(
        "id => expandedBrowseStacks.has(browseStackCoverIdForPhoto(id))",
        selected_id,
    )
    top_after = member.evaluate(
        """card => card.getBoundingClientRect().top -
          document.getElementById('gridContainer').getBoundingClientRect().top"""
    )
    assert abs(top_after - top_before) < 4


def test_entering_a_stack_batch_retires_the_previous_detail_owner(
    live_server, page,
):
    """A batch may not inherit the last focused photo's EXIF suggestion.

    The suggestion element keeps its ``data-photo-id`` and Accept button,
    and an in-flight reverse-geocode uses ``window._detailPhotoId`` as its
    owner check. Leave either behind while entering a stack selection and a
    later batch containing that photo resurrects its Accept line for the
    whole batch — one click would then write one photo's place onto every
    selected photo. Same retirement closeDetail() and clearSelection() have
    done since Codex P2 on PR #1097. Codex P1 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    other_id = live_server["data"]["photos"][3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    other = page.locator(f'.grid-card[data-id="{other_id}"]')

    def focus_then(enter_stack):
        other.click()
        page.wait_for_function(
            "photoId => window._detailPhotoId === photoId", arg=other_id,
        )
        # Stand in for a suggestion the detail panel had painted for it.
        page.evaluate(
            """photoId => {
              var sugg = document.getElementById('locationExifSuggestion');
              sugg.hidden = false;
              sugg.innerHTML = '<button>Accept</button>';
              sugg.dataset.photoId = String(photoId);
            }""",
            other_id,
        )
        enter_stack()
        return page.evaluate(
            """() => {
              var sugg = document.getElementById('locationExifSuggestion');
              return {
                owner: window._detailPhotoId,
                suggestionOwner: sugg.dataset.photoId || null,
                suggestionHidden: sugg.hidden,
              };
            }"""
        )

    # Clicking the collapsed card...
    assert focus_then(lambda: cover.click()) == {
        "owner": None, "suggestionOwner": None, "suggestionHidden": True,
    }
    # ...right-clicking it...
    def right_click():
        cover.click(button="right")
        page.evaluate("() => closeContextMenu()")

    assert focus_then(right_click) == {
        "owner": None, "suggestionOwner": None, "suggestionHidden": True,
    }

    # ...and the tray's Select all, which enters the same batch.
    def tray_select_all():
        cover.locator(".browse-stack-badge").click()
        tray = page.locator(
            f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
        )
        expect(tray.locator(".browse-stack-member")).to_have_count(3)
        tray.get_by_role("button", name="Select all").click()

    assert focus_then(tray_select_all) == {
        "owner": None, "suggestionOwner": None, "suggestionHidden": True,
    }


def test_cover_dropped_from_the_tray_leaves_a_partial_mark(live_server, page):
    """A card may not claim frames a batch action would skip.

    Cmd-clicking the cover out of an expanded stack's tray removes just that
    frame; collapsing then pins single-photo focus to the cover so grid
    navigation resolves in the top-level list. Reading focus and set as an
    "or" painted the whole stack as selected, while ``getActiveSelection()``
    — and so every rating, flag and delete — skipped the very frame on top.
    Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.click()
    expect(cover).to_have_class("grid-card has-browse-stack selected")

    cover.locator(".browse-stack-badge").click()
    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    expect(tray.locator(".browse-stack-member")).to_have_count(3)
    tray.locator(f'.browse-stack-member[data-id="{burst_ids[1]}"]').click(
        modifiers=["Meta"]
    )
    tray.get_by_role("button", name="Collapse stack").click()
    expect(tray).to_be_hidden()

    # Two of three frames are actionable, and the card says so.
    assert page.evaluate("() => getActiveSelection().length") == 2
    expect(page.locator("#batchCount")).to_have_text("2 selected")
    expect(cover).to_have_class("grid-card has-browse-stack stack-partial")


def test_delete_dialog_refuses_a_selection_that_moved_under_it(
    live_server, page,
):
    """The delete dialog has to describe the selection that asked for it.

    Counting companions is a round trip and the grid stays live across it,
    so the user can select something else before the dialog appears — and
    the dialog names only a number, so confirming it would delete photos
    they can no longer see selected. For a disk delete that is not
    recoverable. The unit coverage in test_app.py drives the function
    directly; this walks it through the real grid.
    Codex P1 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    other_id = live_server["data"]["photos"][3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.click()
    expect(page.locator("#batchCount")).to_have_text("3 selected \u00b7 1 stack")

    # Select another photo while the companion count is still in flight —
    # the same window a slow request opens for a real click.
    outcome = page.evaluate(
        """async otherId => {
          var pending = batchDelete();
          var idx = photos.findIndex(function(p) { return p.id === otherId; });
          selectPhoto({shiftKey: false, metaKey: false, ctrlKey: false},
                      otherId, idx);
          await pending;
          return {
            modalOpen: document.getElementById('deleteModal')
              .classList.contains('open'),
            active: getActiveSelection(),
            remaining: photos.length,
          };
        }""",
        other_id,
    )

    assert outcome["modalOpen"] is False
    # Nothing was deleted, and the newer selection is untouched.
    assert outcome["active"] == [other_id]
    assert outcome["remaining"] == 3
    expect(page.locator("#toastContainer")).to_contain_text("Selection changed")


def test_cancelled_delete_does_not_leave_a_stack_gesture_armed(
    live_server, page,
):
    """Cancelling turns the delete's "intermediate" close into the real one.

    Clicking Delete closes the lightbox on its way to the dialog, and a
    gesture held across that close outlives its lightbox when the user
    cancels: nothing reopens, and the next viewing shortcut inherits it. The
    gesture is set aside instead and handed back only by a delete that
    actually happens. Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.dblclick()
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")

    page.locator("#lightboxDeleteBtn").click()
    expect(page.locator("#deleteModal")).to_have_class("modal-overlay open")
    page.locator("#deleteModal button", has_text="Cancel").click()
    expect(page.locator("#deleteModal")).not_to_have_class("modal-overlay open")

    # The stack is still selected, and the abandoned gesture must not be
    # inherited by the next viewing session.
    page.keyboard.press("e")
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")
    page.locator("[title='Next (\u2192)']").click()
    expect(page.locator("#lightboxFilename")).to_have_text("robin1.jpg")
    page.keyboard.press("Escape")
    page.wait_for_timeout(400)
    assert page.evaluate(
        """ids => selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        burst_ids,
    )


def test_badge_double_click_does_not_claim_a_deliberate_batch(live_server, page):
    """The stack badge's clicks never make a selection.

    Both of them expand/collapse the stack and stop propagating, so they
    never reach selectPhoto — but the dblclick still bubbles to the grid and
    opens the lightbox. With an identical id set, that would let a batch the
    user assembled be claimed by a gesture that did not create it.
    Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')

    # Select the stack deliberately, from the tray.
    cover.locator(".browse-stack-badge").click()
    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    tray.get_by_role("button", name="Select all").click()
    tray.get_by_role("button", name="Collapse stack").click()
    expect(tray).to_be_hidden()

    cover.locator(".browse-stack-badge").dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class(
        re.compile(r"\bactive\b")
    )
    page.locator("[title='Next (\u2192)']").click()
    expect(page.locator("#lightboxFilename")).to_have_text("robin1.jpg")
    page.keyboard.press("Escape")
    page.wait_for_timeout(400)
    assert page.evaluate(
        """ids => selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        burst_ids,
    )


def test_delete_dialog_counts_companions_of_unloaded_stack_members(
    live_server, page,
):
    """A stack selected by one click holds frames Browse never loaded.

    Their metadata is not in any member cache, so counting companions in the
    browser saw only the cover: the "Also delete N companion files" checkbox
    never appeared and a disk delete would have left the hidden frames'
    companions on disk. The count comes from the server, which can see every
    row in the selection. Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )
        # The companion belongs to a hidden frame, not the cover.
        db.conn.execute(
            "UPDATE photos SET companion_path = ? WHERE id = ?",
            ("hawk3.nef", burst_ids[2]),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.click()
    expect(page.locator("#batchCount")).to_have_text("3 selected \u00b7 1 stack")
    assert page.evaluate("() => Object.keys(browseStackMembers).length") == 0

    page.locator("#batchBar button", has_text="Delete").click()
    expect(page.locator("#deleteModal")).to_have_class("modal-overlay open")
    expect(page.locator("#deleteCompanionRow")).to_be_visible()
    expect(page.locator("#deleteCompanionLabel")).to_have_text(
        "Also delete 1 companion file"
    )
    page.locator("#deleteModal button", has_text="Cancel").click()


def test_undo_keeps_a_whole_stack_selected(live_server, page):
    """Undo reloads the grid, and the selection has to survive it whole.

    Restoring a selection drops ids the refreshed query no longer has, which
    it decides with ``findBrowsePhoto``. For a stack selected by one click on
    its collapsed card, the hidden frames are in no member cache, so that
    lookup failed for all but the cover and the stack quietly shrank to one
    frame — the next rating or flag would then hit one photo instead of
    three. Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.click()
    expect(page.locator("#batchCount")).to_have_text("3 selected \u00b7 1 stack")

    # A colour label edits every frame without moving any of them between
    # stacks, so the reload that follows the undo must hand the whole
    # selection back.
    page.evaluate("() => batchSetColorLabel('red')")
    page.wait_for_function(
        "ids => ids.every(function(id) { return colorLabels[id] === 'red'; })",
        arg=burst_ids,
    )
    page.evaluate("() => doUndo()")
    page.wait_for_function(
        """ids => selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        arg=burst_ids,
    )
    expect(page.locator("#batchCount")).to_have_text("3 selected \u00b7 1 stack")


def test_undo_hydration_does_not_clobber_a_fresh_selection(live_server, page):
    """Selecting mid-hydration must not resurrect the pre-undo ids.

    ``afterHistoryChange`` captures the selection before ``resetAndLoad``,
    then hydrates every cover whose members it might need. An uncached
    collapsed stack is exactly that case: its hidden frames have never
    been fetched, so hydration awaits ``/api/photos/by-ids`` before the
    restore loop can find them. If the user clicks another card during
    that await, the restore would fold the pre-undo stack back into the
    fresh selection, replacing a one-card pick or merging into a new
    batch. ``anchorRestoreEpoch`` moves on every selection change, so the
    handler snapshots it after the reload and refuses to restore when the
    snapshot no longer matches. Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    other_id = live_server["data"]["photos"][3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.click()
    expect(page.locator("#batchCount")).to_have_text("3 selected · 1 stack")
    # The stack has never been expanded, so its hidden frames are in no
    # member cache — the case the restore hydrates for.
    assert page.evaluate("() => Object.keys(browseStackMembers).length") == 0

    # Colour label edits every frame in the selection but does not move
    # any of them between stacks, so the reload's own restore is what
    # would put the stack back — the same handler under test.
    page.evaluate("() => batchSetColorLabel('red')")
    page.wait_for_function(
        "ids => ids.every(function(id) { return colorLabels[id] === 'red'; })",
        arg=burst_ids,
    )

    # Put the click inside the restore's async window by construction rather
    # than by timing, so the race runs on every machine. The seam is the
    # hydration call itself: wrapping window.fetch would do nothing here,
    # because vireo-api.js binds the native fetch at load and replaces the
    # global, so app requests never see a later patch.
    page.evaluate(
        """otherId => {
          var orig = hydrateBrowseStackCoverMembers;
          window.__pickedDuringHydration = false;
          hydrateBrowseStackCoverMembers = function(cover, windowIsCurrent) {
            if (!window.__pickedDuringHydration) {
              window.__pickedDuringHydration = true;
              var idx = photos.findIndex(function(p) { return p.id === otherId; });
              selectPhoto({shiftKey: false, metaKey: false, ctrlKey: false},
                          otherId, idx);
            }
            return orig.apply(this, arguments);
          };
          window.__restoreHydrate = function() {
            hydrateBrowseStackCoverMembers = orig;
            delete window.__restoreHydrate;
          };
        }""",
        other_id,
    )

    try:
        page.evaluate("async () => { await doUndo(); }")
    finally:
        page.evaluate("() => window.__restoreHydrate && window.__restoreHydrate()")

    # The restore ran after the user had already chosen a card. A stale
    # restore would push the burst ids back into selectedPhotos, and the bar
    # would count them — a single-card pick leaves it reading one photo, with
    # no stack note, since one card is still an actionable selection.
    assert page.evaluate("() => window.__pickedDuringHydration") is True
    assert page.evaluate("() => getActiveSelection()") == [other_id]
    assert page.evaluate("() => selectedPhotoId") == other_id
    expect(page.locator("#batchCount")).to_have_text("1 selected")


def test_cmd_clicking_a_selected_stack_deselects_it_as_a_unit(live_server, page):
    """Toggling a stack off has to take its focus with it.

    Collapsing an expanded stack pins single-photo focus to the cover so
    grid and preview navigation resolve in the top-level list. A Cmd-click
    that then removes every frame emptied ``selectedPhotos`` but left that
    focus behind, so ``getActiveSelection()`` fell back to the cover: a
    stack the user deselected as a unit came back as a one-photo partial
    selection. Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.click()
    expect(page.locator("#batchCount")).to_have_text("3 selected \u00b7 1 stack")

    # Expand and collapse: the batch survives, with focus pinned to the cover.
    cover.locator(".browse-stack-badge").click()
    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    expect(tray.locator(".browse-stack-member")).to_have_count(3)
    tray.get_by_role("button", name="Collapse stack").click()
    page.wait_for_function(
        "coverId => selectedPhotoId === coverId && selectedPhotos.size === 3",
        arg=burst_ids[1],
    )

    cover.click(modifiers=["Meta"])
    page.wait_for_function(
        "() => getActiveSelection().length === 0 && selectedPhotoId === null"
    )
    expect(page.locator("#batchBar")).to_be_hidden()
    expect(cover).to_have_class("grid-card has-browse-stack")


def test_deleting_a_stacks_cover_in_the_lightbox_drops_its_dangling_members(
    live_server, page,
):
    """A stack's cover is the only card its hidden members have.

    Deleting it from the lightbox removes just that id from the selection,
    which would leave the rest of the double-click's stack selected with
    nothing on screen representing them — the next rating or delete
    shortcut would act on photos the user cannot see. A selection that only
    ever shrank is still the gesture's own, so the close handler reconciles
    it instead of mistaking it for an assembled batch.
    Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    other_id = live_server["data"]["photos"][3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.dblclick()
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")

    page.locator("#lightboxDeleteBtn").click()
    expect(page.locator("#deleteModal")).to_have_class("modal-overlay open")
    page.locator("#deleteConfirmBtn").click()
    expect(page.locator("#deleteModal")).not_to_have_class("modal-overlay open")
    expect(page.locator("#lightboxFilename")).to_have_text("robin1.jpg")
    page.keyboard.press("Escape")

    page.wait_for_function(
        "photoId => selectedPhotoId === photoId && selectedPhotos.size === 0",
        arg=other_id,
    )
    assert page.evaluate("() => getActiveSelection()") == [other_id]


def test_lightbox_navigation_follows_a_double_clicked_stack(live_server, page):
    """A double-click on a stack card is a viewing gesture, not a batch.

    Its two clicks run through ``selectPhoto`` before the lightbox opens, so
    they leave the stack selected. The close handler preserves an existing
    batch rather than replacing it with the viewed photo — correct for a
    batch the user assembled card by card, wrong for the selection the
    opening gesture itself just made, which would strand the grid on the
    stack after the user navigated away and closed. A selection that is
    exactly one stack may be replaced; finishing inside that same stack
    leaves it alone rather than shrinking it to one frame.
    Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    other_id = live_server["data"]["photos"][3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    expect(cover).to_be_visible()

    # Close on the frame the gesture opened: the stack stays selected.
    cover.dblclick()
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")
    page.keyboard.press("Escape")
    page.wait_for_function(
        """ids => selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        arg=burst_ids,
    )

    # Navigate out of the stack, and the grid follows the user home. The
    # gesture has to be a fresh one: a double-click over a stack that was
    # already selected reaffirms that batch rather than creating it, and
    # batches are preserved (see the Select all case below).
    page.evaluate("() => clearSelection()")
    cover.dblclick()
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")
    page.locator("[title='Next (\u2192)']").click()
    expect(page.locator("#lightboxFilename")).to_have_text("robin1.jpg")
    page.keyboard.press("Escape")
    page.wait_for_function(
        "photoId => selectedPhotoId === photoId && selectedPhotos.size === 0",
        arg=other_id,
    )
    expect(page.locator("#detailFilename")).to_have_text("robin1.jpg")

    # The gesture is spent by its own close. Re-opening the same, untouched
    # stack selection with a viewing shortcut afterwards is a viewing shortcut
    # over a batch, so the batch is preserved.
    cover.dblclick()
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")
    page.keyboard.press("Escape")
    page.wait_for_function(
        """ids => selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        arg=burst_ids,
    )
    page.keyboard.press("e")
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")
    page.locator("[title='Next (\u2192)']").click()
    expect(page.locator("#lightboxFilename")).to_have_text("robin1.jpg")
    page.keyboard.press("Escape")
    page.wait_for_timeout(400)
    assert page.evaluate(
        """ids => selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        burst_ids,
    )

    # Same ids, different provenance: a stack the user selected on purpose is
    # a batch, and viewing it with a shortcut leaves it alone even when the
    # user navigates away and closes somewhere else entirely.
    cover.locator(".browse-stack-badge").click()
    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    tray.get_by_role("button", name="Select all").click()
    tray.get_by_role("button", name="Collapse stack").click()
    expect(tray).to_be_hidden()
    page.keyboard.press("e")
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")
    page.locator("[title='Next (\u2192)']").click()
    expect(page.locator("#lightboxFilename")).to_have_text("robin1.jpg")
    page.keyboard.press("Escape")
    page.wait_for_timeout(400)
    assert page.evaluate(
        """ids => selectedPhotos.size === ids.length
          && ids.every(function(id) { return selectedPhotos.has(id); })""",
        burst_ids,
    )


def test_double_clicking_a_preselected_stack_preserves_the_batch(
    live_server, page,
):
    """A dblclick on a stack the user already selected is a viewing gesture.

    Its two clicks reaffirm the pre-existing selection rather than making a
    new one, so the resulting ``selectedPhotos`` is byte-for-byte identical
    to the gesture-generated case — a membership check at dblclick time
    cannot tell them apart. Without capturing the pre-first-click state, the
    close handler would consume the marker and silently replace the user's
    tray Select all batch with the photo they navigated to.
    Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    expect(cover).to_be_visible()

    # Assemble a deliberate batch through the tray, then collapse.
    cover.locator(".browse-stack-badge").click()
    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    tray.get_by_role("button", name="Select all").click()
    tray.get_by_role("button", name="Collapse stack").click()
    expect(tray).to_be_hidden()
    page.wait_for_function(
        "ids => selectedPhotos.size === ids.length"
        " && ids.every(function(id) { return selectedPhotos.has(id); })",
        arg=burst_ids,
    )

    # Double-click the collapsed cover — a viewing gesture over an existing
    # batch, not the batch itself. Navigating away and closing must leave
    # the user's deliberate selection intact.
    cover.dblclick()
    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")
    page.locator("[title='Next (→)']").click()
    expect(page.locator("#lightboxFilename")).to_have_text("robin1.jpg")
    page.keyboard.press("Escape")
    page.wait_for_timeout(400)
    assert page.evaluate(
        "ids => selectedPhotos.size === ids.length"
        " && ids.every(function(id) { return selectedPhotos.has(id); })",
        burst_ids,
    )


def test_clearing_the_selection_scrubs_a_stack_cards_partial_mark(
    live_server, page,
):
    """Clear has to repaint from the selection, not strip one class by hand.

    A frame picked out of a tray leaves its collapsed cover carrying the
    dashed partial mark. A hand-rolled scrub that only knew about
    ``selected`` left that mark on a grid with nothing selected — a card
    still claiming a selection the batch bar had already dropped.
    Codex P2 on PR #1672.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    expect(cover).to_be_visible()

    cover.locator(".browse-stack-badge").click()
    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    expect(tray.locator(".browse-stack-member")).to_have_count(3)
    tray.locator(f'.browse-stack-member[data-id="{burst_ids[2]}"]').click()
    tray.get_by_role("button", name="Collapse stack").click()
    expect(tray).to_be_hidden()
    expect(cover).to_have_class("grid-card has-browse-stack stack-partial")

    page.locator("#batchBar button", has_text="Clear").click()
    expect(page.locator("#batchBar")).to_be_hidden()
    expect(cover).to_have_class("grid-card has-browse-stack")


def test_expanded_stack_paints_its_members_once(live_server, page):
    """A tray's members are rendered once, not again per metadata response.

    Expanding a stack paints its members, then two metadata requests (iNat
    badges, colour labels) resolve a beat later. Re-rendering the tray for
    each of those hands every member a fresh <img>, so a thumbnail the
    browser had already decoded blanks and redownloads — and any click the
    user has begun on a member lands on a node that no longer exists.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    hidden_member_id = burst_ids[0]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )
    # A hidden member only learns its iNat state from the request the
    # expansion fires, so this badge appearing is proof that metadata landed
    # on a card that was already on screen.
    db.record_inat_submission(
        hidden_member_id, 12345, "https://www.inaturalist.org/observations/12345"
    )

    page.goto(f"{live_server['url']}/browse")
    # Count member cards as they are created rather than comparing before and
    # after: the metadata responses land within milliseconds of the tray, far
    # inside one round trip from the test.
    page.evaluate(
        """() => {
          window.__memberNodesCreated = 0;
          new MutationObserver(function(records) {
            records.forEach(function(record) {
              Array.prototype.forEach.call(record.addedNodes, function(node) {
                if (node.nodeType !== 1) return;
                if (node.classList.contains('browse-stack-member')) {
                  window.__memberNodesCreated++;
                }
                window.__memberNodesCreated +=
                  node.querySelectorAll('.browse-stack-member').length;
              });
            });
          }).observe(document.body, {childList: true, subtree: true});
        }"""
    )

    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.locator(".browse-stack-badge").click()

    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    expect(tray.locator(".browse-stack-member")).to_have_count(3)
    expect(
        tray.locator(f'.browse-stack-member[data-id="{hidden_member_id}"] .inat-badge')
    ).to_be_visible()
    # The colour-label request resolves independently of the iNat one; give it
    # room to land before counting.
    page.wait_for_timeout(500)
    assert page.evaluate("() => window.__memberNodesCreated") == 3


def test_stack_metadata_callbacks_follow_promoted_cover(live_server, page):
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    callback_refreshes = page.evaluate(
        """async ids => {
          var oldCoverId = ids[1];
          var promotedId = ids[0];
          // The colour dot only renders for someone who has the colour field
          // on their cards, which this profile does not by default.
          if (cardFields.indexOf('color_label') === -1) {
            cardFields = cardFields.concat(['color_label']);
          }
          var originalLoadInatStatus = loadInatStatus;
          var originalFetchColorLabels = fetchColorLabels;
          var releaseInat;
          var releaseColors;
          // Each fake lands its metadata the way the real fetch does — into
          // the shared map the cards read — so the assertions below are about
          // what the member card shows, not about which function repainted it.
          loadInatStatus = function() {
            return new Promise(function(resolve) {
              releaseInat = function() {
                inatSubmitted[String(promotedId)] = true;
                resolve();
              };
            });
          };
          fetchColorLabels = function() {
            return new Promise(function(resolve) {
              releaseColors = function() {
                colorLabels[promotedId] = 'red';
                resolve();
              };
            });
          };
          try {
            await toggleBrowseStack(null, oldCoverId);
            var members = browseStackMembers[String(oldCoverId)];
            members.find(function(photo) { return photo.id === oldCoverId; }).flag = 'rejected';
            members.find(function(photo) { return photo.id === promotedId; }).flag = 'flagged';
            await reconcileBrowseStackCovers([oldCoverId, promotedId]);

            var currentCoverId = browseStackCoverIdForPhoto(oldCoverId);
            var memberSelector = '.browse-stack-tray[data-stack-cover-id="'
              + currentCoverId + '"] .browse-stack-member[data-id="'
              + promotedId + '"]';
            // Tag the member node itself: a repaint has to reach this card
            // without replacing it, or a thumbnail it already decoded — and a
            // click the user is halfway through — go with the old node.
            document.querySelector(memberSelector).dataset.sameNode = '1';

            releaseInat();
            await new Promise(function(resolve) { setTimeout(resolve, 0); });
            var inatRefreshed = !!document.querySelector(
              memberSelector + ' .inat-badge'
            );

            releaseColors();
            await new Promise(function(resolve) { setTimeout(resolve, 0); });
            var colorsRefreshed = !!document.querySelector(
              memberSelector + ' .grid-card-color[data-color="red"]'
            );
            return {
              currentCoverId: currentCoverId,
              inatRefreshed: inatRefreshed,
              colorsRefreshed: colorsRefreshed,
              memberKeptItsNode: document.querySelector(memberSelector)
                .dataset.sameNode === '1',
            };
          } finally {
            loadInatStatus = originalLoadInatStatus;
            fetchColorLabels = originalFetchColorLabels;
          }
        }""",
        burst_ids,
    )
    assert callback_refreshes == {
        "currentCoverId": burst_ids[0],
        "inatRefreshed": True,
        "colorsRefreshed": True,
        "memberKeptItsNode": True,
    }


def test_concurrent_stack_hydration_uses_newest_request(live_server, page):
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()
    assert page.evaluate("() => Object.keys(browseStackMembers).length") == 0

    hydration_state = page.evaluate(
        """async ids => {
          var oldCoverId = ids[1];
          var promotedId = ids[0];
          var originalSafeFetch = safeFetch;
          var options = {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({photo_ids: ids}),
          };
          var baseline = await originalSafeFetch('/api/photos/by-ids', options);
          var releases = [];
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids') {
              return new Promise(function(resolve) { releases.push(resolve); });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            var first = reconcileBrowseStackCovers([oldCoverId]);
            var second = reconcileBrowseStackCovers([oldCoverId]);
            while (releases.length < 2) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }

            // The older response arrives first, but the second request was
            // issued after a newer mutation and must remain authoritative.
            releases[0](JSON.parse(JSON.stringify(baseline)));
            await first;
            var keysAfterOlderResponse = Object.keys(browseStackMembers);

            var newerResponse = JSON.parse(JSON.stringify(baseline));
            newerResponse.photos.forEach(function(photo) {
              if (photo.id === oldCoverId) photo.flag = 'rejected';
              if (photo.id === promotedId) photo.flag = 'flagged';
            });
            releases[1](newerResponse);
            await second;
            return {
              keysAfterOlderResponse: keysAfterOlderResponse,
              cacheKeys: Object.keys(browseStackMembers).map(Number),
              currentCoverId: browseStackCoverIdForPhoto(oldCoverId),
              gridHasPromotedCover: photos.some(function(photo) {
                return photo.id === promotedId && !!photo.browse_stack;
              }),
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    assert hydration_state == {
        "keysAfterOlderResponse": [],
        "cacheKeys": [burst_ids[0]],
        "currentCoverId": burst_ids[0],
        "gridHasPromotedCover": True,
    }


def test_shift_range_from_stack_member_keeps_selection_honest(live_server, page):
    """A Shift-range anchored on an expanded member must not retarget actions.

    Clicking a hidden stack member anchors selectedIndex on the *cover's* grid
    slot. Shift-clicking another card then built a range that contained the
    cover but not the focused member, while the member's card kept rendering
    as selected — so the batch bar, Export and Delete acted on a photo the
    user could see was not the highlighted one.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    other_ids = live_server["data"]["photos"][3:]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator("#grid > .grid-card")).to_have_count(3)

    stack_card = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    stack_card.locator(".browse-stack-badge").click()
    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    expect(tray.locator(".browse-stack-member")).to_have_count(3)

    hidden_member = tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[2]}"]'
    )
    hidden_member.click()
    expect(hidden_member).to_have_class("browse-stack-member selected")

    page.locator(f'.grid-card[data-id="{other_ids[-1]}"]').click(
        modifiers=["Shift"]
    )

    selection_state = page.evaluate(
        """() => {
          function ids(nodes) {
            return Array.from(new Set(Array.from(nodes).map(function(el) {
              return parseInt(el.dataset.id, 10);
            }))).sort(function(a, b) { return a - b; });
          }
          return {
            active: getActiveSelection().slice().sort(function(a, b) { return a - b; }),
            highlighted: ids(document.querySelectorAll(
              '.grid-card.selected, .browse-stack-member.selected'
            )),
          };
        }"""
    )
    # What is highlighted is exactly what a batch action would target.
    assert selection_state["active"] == selection_state["highlighted"]
    # And the member the user actually clicked is still one of them.
    assert burst_ids[2] in selection_state["active"]
    expect(hidden_member).to_have_class("browse-stack-member selected")
    # The range swept one whole stack and two singles, and the bar says so:
    # a stack card in a Shift-range contributes every frame behind it.
    expect(page.locator("#batchCount")).to_have_text(
        str(len(selection_state["active"])) + " selected \u00b7 1 stack"
    )

    # The export modal snapshots the active selection, so the focused member
    # has to survive into the job the user actually confirms.
    page.locator("#batchBar button", has_text="Export").click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    export_ids = page.evaluate(
        "() => (_exportPhotoIds || getActiveSelection()).slice()"
    )
    assert sorted(export_ids) == selection_state["active"]
    page.locator("#exportOverlay button", has_text="Cancel").click()


def test_stack_edit_during_expansion_outlives_the_pending_response(live_server, page):
    """A stack-wide edit applied while the tray is loading must survive.

    The tray header offers "Select all" the moment it opens, so a user can
    apply Wildlife Exclude to every member before the expansion's
    ``/api/photos/by-ids`` response lands. Those hidden members are in neither
    ``photos`` nor ``browseStackMembers`` yet, so ``findBrowsePhoto()`` cannot
    patch them — and installing the already-in-flight response then repainted
    pre-edit values in the tray and its "No Wildlife" badges, contradicting an
    edit the user had just confirmed, until a full reload.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    state = page.evaluate(
        """async ids => {
          var coverId = ids[1];
          var originalSafeFetch = safeFetch;
          var byIdsOptions = {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({photo_ids: ids}),
          };
          // The payload the in-flight expansion would carry: pre-edit.
          var preEdit = await originalSafeFetch('/api/photos/by-ids', byIdsOptions);
          var release = null;
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids' && !release) {
              return new Promise(function(resolve) { release = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            var expansion = toggleBrowseStack(null, coverId);
            while (!release) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            var loadingWhileSelectable = !!document.querySelector(
              '.browse-stack-tray[data-stack-cover-id="' + coverId + '"] .browse-stack-loading'
            );
            selectBrowseStackAll(null, coverId);
            var selected = getActiveSelection().slice().sort(function(a, b) { return a - b; });
            await setSelectionWildlifeExcluded(true);
            // Only now does the response fetched before the edit arrive.
            release(JSON.parse(JSON.stringify(preEdit)));
            await expansion;
            return {
              loadingWhileSelectable: loadingWhileSelectable,
              selected: selected,
              cached: (browseStackMembers[String(coverId)] || []).map(function(photo) {
                return [photo.id, photo.wildlife_excluded ? 1 : 0];
              }).sort(function(a, b) { return a[0] - b[0]; }),
              badges: document.querySelectorAll(
                '.browse-stack-tray[data-stack-cover-id="' + coverId + '"] .no-wildlife-badge'
              ).length,
              error: browseStackErrors[String(coverId)] || null,
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    # The edit was reachable while the members were still loading.
    assert state["loadingWhileSelectable"] is True
    assert state["selected"] == sorted(burst_ids)
    assert state["error"] is None
    # Every member — cover and hidden alike — reflects the edit the user made.
    assert state["cached"] == [[pid, 1] for pid in sorted(burst_ids)]
    assert state["badges"] == 3


def test_stack_expansion_response_yields_to_fresher_hydration(live_server, page):
    """An in-flight expansion must not overwrite a post-edit hydration.

    ``reconcileBrowseStackCovers()`` hydrates an uncached stack *after* the
    edit that triggered it, so its cache is strictly fresher than an expansion
    request issued before that edit. The expansion's unconditional write used
    to clobber it and put the pre-edit ratings back in the tray.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    state = page.evaluate(
        """async ids => {
          var coverId = ids[1];
          var originalSafeFetch = safeFetch;
          var byIdsOptions = {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({photo_ids: ids}),
          };
          var preEdit = await originalSafeFetch('/api/photos/by-ids', byIdsOptions);
          var held = [];
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids') {
              return new Promise(function(resolve) { held.push(resolve); });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            var expansion = toggleBrowseStack(null, coverId);
            while (held.length < 1) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            selectBrowseStackAll(null, coverId);
            var rated = batchSetRating(4);
            // batchSetRating -> reconcileBrowseStackCovers issues the second
            // by-ids request, this one after the rating committed.
            while (held.length < 2) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            held[1](await originalSafeFetch('/api/photos/by-ids', byIdsOptions));
            await rated;
            // The pre-edit expansion response finally arrives last.
            held[0](JSON.parse(JSON.stringify(preEdit)));
            await expansion;
            return {
              cached: (browseStackMembers[String(coverId)] || []).map(function(photo) {
                return [photo.id, photo.rating];
              }).sort(function(a, b) { return a[0] - b[0]; }),
              error: browseStackErrors[String(coverId)] || null,
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    assert state["error"] is None
    assert state["cached"] == [[pid, 4] for pid in sorted(burst_ids)]


def test_generic_member_edit_invalidates_pending_cover_hydration(live_server, page):
    """A generic member edit during hydration must not let pre-edit data land.

    When Select all matching includes an uncached collapsed stack, a rating or
    flag edit starts a cover hydration via ``reconcileBrowseStackCovers``. If a
    concurrent wildlife or keyword edit completes before the hydration's
    ``/api/photos/by-ids`` response arrives, its mutation path only ran through
    ``refreshExpandedBrowseStackMembers``, which used to invalidate expansion
    requests but not the pending hydration. The delayed response then wrote
    pre-edit member objects into ``browseStackMembers`` and later expansion
    showed stale badges until reload.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    outcome = page.evaluate(
        """async ids => {
          var coverId = ids[1];
          var hiddenId = ids[0];
          var originalSafeFetch = safeFetch;
          var byIdsOptions = {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({photo_ids: ids}),
          };
          var preEdit = await originalSafeFetch('/api/photos/by-ids', byIdsOptions);
          // Tag the in-flight payload so a pre-edit row is recognisable if it
          // ever reaches the cache.
          var stalePayload = JSON.parse(JSON.stringify(preEdit));
          stalePayload.photos.forEach(function(photo) {
            photo.filename = 'PRE-EDIT.jpg';
          });
          var release = null;
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids' && !release) {
              return new Promise(function(resolve) { release = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            // Rating edit routed through reconcileBrowseStackCovers begins the
            // uncached cover's hydration; the /api/photos/by-ids response is
            // captured for release after the wildlife edit lands.
            var hydration = reconcileBrowseStackCovers([hiddenId]);
            while (!release) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            // Generic member mutation whose only cache-invalidation hook is
            // refreshExpandedBrowseStackMembers. It must mark the pending
            // hydration so the delayed response cannot land.
            refreshExpandedBrowseStackMembers([hiddenId]);
            release(stalePayload);
            await hydration;
            var cached = browseStackMembers[String(coverId)] || [];
            return {
              filenames: cached.map(function(photo) {
                return photo.filename;
              }).sort(),
              error: browseStackErrors[String(coverId)] || null,
              recheck: browseStackCoverRecheck.has(coverId),
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    assert outcome["error"] is None
    # The pre-edit payload never reaches the cache...
    assert "PRE-EDIT.jpg" not in outcome["filenames"]
    # ...and the reconciliation is not abandoned either: it refetches the
    # post-edit members so the cover it exists to recompute still gets one.
    assert outcome["filenames"] == ["hawk1.jpg", "hawk2.jpg", "hawk3.jpg"]
    assert outcome["recheck"] is False


def test_cover_hydration_retries_transient_by_ids_failure(live_server, page):
    """A failed hydration must not abandon a reconciliation that was earned.

    The flag edit below already committed on the server. If the follow-up
    ``/api/photos/by-ids`` fails transiently and reconciliation just returns,
    the grid keeps showing the demoted cover with no path back short of a
    reload, because later expanding the stack only fills the member cache
    without recomputing the cover.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    outcome = page.evaluate(
        """async ids => {
          var coverId = ids[1];
          var hiddenId = ids[0];
          var originalSafeFetch = safeFetch;
          var failed = 0;
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids' && failed < 1) {
              failed++;
              return Promise.reject(new Error('transient'));
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            // Flagging a hidden member makes it outrank the quality-picked
            // cover, so a completed reconciliation must promote it.
            await setFlagFor(hiddenId, 'flagged');
            return {
              failed: failed,
              coverIds: photos.filter(function(photo) {
                return !!photo.browse_stack;
              }).map(function(photo) { return photo.id; }),
              recheck: browseStackCoverRecheck.has(coverId),
              toasts: document.getElementById('toastContainer').textContent,
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    assert outcome["failed"] == 1
    # The retry succeeded, so the promotion the edit earned actually happened.
    assert outcome["coverIds"] == [burst_ids[0]]
    assert outcome["recheck"] is False
    assert "Could not reload" not in outcome["toasts"]


def test_unresolvable_cover_hydration_is_reported_and_recoverable(live_server, page):
    """When retries are exhausted the stale cover must not be shown silently.

    The mutation committed, so the grid is knowingly displaying a photo the
    stack may no longer lead with. The user gets told, the badge keeps saying
    so after the toast fades, and expanding the stack recomputes the cover.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    outcome = page.evaluate(
        """async ids => {
          var coverId = ids[1];
          var hiddenId = ids[0];
          var originalSafeFetch = safeFetch;
          var attempts = 0;
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids') {
              attempts++;
              return Promise.reject(new Error('offline'));
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            await setFlagFor(hiddenId, 'flagged');
            return {
              attempts: attempts,
              recheck: browseStackCoverRecheck.has(coverId),
              toasts: document.getElementById('toastContainer').textContent,
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    assert outcome["attempts"] == 3
    assert outcome["recheck"] is True
    assert "Could not reload a stack after that edit" in outcome["toasts"]

    # The badge carries the notice after the toast is gone.
    badge = page.locator(f'.grid-card[data-id="{burst_ids[1]}"] .browse-stack-badge')
    expect(badge).to_have_class(re.compile("needs-recheck"))
    assert "cover may be out of date" in (badge.get_attribute("title") or "")

    # Expanding is the advertised recovery: members load, the cover is
    # recomputed from them, and the marker clears.
    badge.click()
    promoted = page.locator(f'.grid-card[data-id="{burst_ids[0]}"] .browse-stack-badge')
    expect(promoted).to_be_visible()
    expect(promoted).not_to_have_class(re.compile("needs-recheck"))
    tray = page.locator(f'.browse-stack-tray[data-stack-cover-id="{burst_ids[0]}"]')
    expect(tray.locator(".browse-stack-member")).to_have_count(3)
    assert page.evaluate("id => browseStackCoverRecheck.has(id)", burst_ids[1]) is False


def test_expanding_stack_over_500_chunks_by_ids_requests(live_server, page):
    """A stack with more than 500 members must be expandable.

    ``/api/photos/by-ids`` caps each POST at 500 ids, so the expansion path
    used to bail with a permanent ``This stack is too large to expand in
    Browse.`` error on the badge for anything over that. Cover reconciliation
    already chunks in 500-id slices; the expansion path must do the same so
    the tray actually shows every member instead of leaving the stack
    permanently unexpandable.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    outcome = page.evaluate(
        """async coverId => {
          var cover = photos.find(function(photo) { return photo.id === coverId; });
          // Simulate a stack whose photo_ids list exceeds the /api/photos/by-ids
          // 500-id cap. The server-side ordering is irrelevant to this test;
          // what matters is that the client stops surfacing a permanent
          // "too large" error and chunks its fetches instead.
          cover.browse_stack.photo_ids = Array(501).fill(coverId);
          delete browseStackMembers[String(coverId)];
          delete browseStackErrors[String(coverId)];
          expandedBrowseStacks.delete(coverId);
          var originalSafeFetch = safeFetch;
          var chunks = [];
          safeFetch = function(url, options) {
            if (url === '/api/photos/by-ids') {
              chunks.push(JSON.parse(options.body).photo_ids.length);
              return Promise.resolve({photos: [cover]});
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            await toggleBrowseStack(null, coverId);
          } finally {
            safeFetch = originalSafeFetch;
          }
          return {
            chunks: chunks,
            error: browseStackErrors[String(coverId)] || null,
            cached: !!browseStackMembers[String(coverId)],
            expanded: expandedBrowseStacks.has(coverId),
          };
        }""",
        burst_ids[1],
    )
    assert outcome["chunks"] == [500, 1]
    assert outcome["error"] is None
    assert outcome["cached"] is True
    assert outcome["expanded"] is True


def test_recollapse_and_reexpand_marks_all_expansion_requests_stale(live_server, page):
    """Every in-flight stack expansion must be markable stale, not just the newest.

    Collapsing the tray does not cancel a pending ``/api/photos/by-ids`` request,
    and a subsequent re-expand starts a new one. The pending-request map used to
    overwrite its bookkeeping to the second request, so a stack-wide edit could
    reach only the newest request via ``markBrowseStackExpansionsStale``; the
    first response then wrote pre-edit members into ``browseStackMembers``.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    state = page.evaluate(
        """async ids => {
          var coverId = ids[1];
          var originalSafeFetch = safeFetch;
          var held = [];
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids') {
              return new Promise(function(resolve) { held.push(resolve); });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            var expansion1 = toggleBrowseStack(null, coverId);
            while (held.length < 1) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            var trackedAfterFirst = (
              browseStackExpansionRequests[String(coverId)] || []
            ).length;
            // Collapse does not cancel the in-flight request.
            await toggleBrowseStack(null, coverId);
            var expansion2 = toggleBrowseStack(null, coverId);
            while (held.length < 2) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            var trackedList = browseStackExpansionRequests[String(coverId)] || [];
            var trackedAfterReexpand = trackedList.length;
            var allNotStale = trackedList.every(function(r) { return !r.stale; });
            // Simulate a stack-wide edit that runs while both requests are
            // in flight (setSelectionWildlifeExcluded and friends all funnel
            // through markBrowseStackExpansionsStale for exactly this case).
            markBrowseStackExpansionsStale(ids);
            var allStaleAfterMark = trackedList.every(function(r) { return r.stale; });
            // Settle every pending promise so the outer expansions terminate.
            // Retries after markStale will push additional resolves, so drain
            // until nothing new appears (bounded by settleGuard as a safety net).
            var settleGuard = 0;
            while (held.length && settleGuard++ < 40) {
              var batch = held.slice();
              held.length = 0;
              batch.forEach(function(resolve) { resolve({photos: []}); });
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            try { await expansion1; } catch (e) {}
            try { await expansion2; } catch (e) {}
            return {
              trackedAfterFirst: trackedAfterFirst,
              trackedAfterReexpand: trackedAfterReexpand,
              allNotStale: allNotStale,
              allStaleAfterMark: allStaleAfterMark,
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    assert state["trackedAfterFirst"] == 1
    # Both the pre-collapse request and the post-reexpand request must be
    # tracked concurrently — otherwise the earlier one is invisible to
    # markBrowseStackExpansionsStale and can cache pre-edit members.
    assert state["trackedAfterReexpand"] == 2
    assert state["allNotStale"] is True
    assert state["allStaleAfterMark"] is True


def test_shift_click_stack_member_from_grid_anchor_range_selects(live_server, page):
    """Shift-clicking a hidden stack member from a top-level anchor must range-select.

    The click handler used to force ``shiftKey=false`` through
    ``selectPhoto``, so the modifier was silently dropped and the click became
    a single-select of the member. The reverse direction (member anchor plus
    Shift-click on a grid card) already range-selects; this direction has to
    match — and the clicked member must land in the resulting range even
    though it is not in the top-level ``photos`` array.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    other_ids = live_server["data"]["photos"][3:]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator("#grid > .grid-card")).to_have_count(3)

    stack_card = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    stack_card.locator(".browse-stack-badge").click()
    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    expect(tray.locator(".browse-stack-member")).to_have_count(3)

    # Anchor on a top-level card outside the stack.
    page.locator(f'.grid-card[data-id="{other_ids[0]}"]').click()

    hidden_member = tray.locator(
        f'.browse-stack-member[data-id="{burst_ids[2]}"]'
    )
    hidden_member.click(modifiers=["Shift"])

    selection_state = page.evaluate(
        """() => {
          function ids(nodes) {
            return Array.from(new Set(Array.from(nodes).map(function(el) {
              return parseInt(el.dataset.id, 10);
            }))).sort(function(a, b) { return a - b; });
          }
          return {
            active: getActiveSelection().slice().sort(function(a, b) { return a - b; }),
            highlighted: ids(document.querySelectorAll(
              '.grid-card.selected, .browse-stack-member.selected'
            )),
          };
        }"""
    )
    # The clicked hidden member is in the selection despite not being a
    # top-level card — otherwise the range loop can only reach the cover.
    assert burst_ids[2] in selection_state["active"]
    # The pre-click top-level anchor is still selected (this is a range,
    # not a single-select replacement).
    assert other_ids[0] in selection_state["active"]
    # What is highlighted equals what a batch action will act on.
    assert selection_state["active"] == selection_state["highlighted"]
    expect(hidden_member).to_have_class("browse-stack-member selected")


def test_export_preview_resolves_unloaded_stack_member(live_server, page):
    """The export preview must name the photo that will actually be written.

    Select-all-matching (and the stack "select all" action) puts hidden
    member ids in the selection, but ``browseStackMembers`` only carries
    trays the user expanded, so the first selected id is often absent from
    the grid, the tray caches, and the lightbox cache. Previewing the
    stack's loaded cover instead asserts a filename the export will never
    write — a plausible-looking stand-in is worse than no preview. Resolve
    the real photo (Codex P2 on PR #1561).
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        # hawk2 becomes the quality-ranked cover; hawk1/hawk3 stay hidden.
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator("#grid > .grid-card")).to_have_count(3)

    # The stack is collapsed, so the hidden member is in no client cache.
    assert page.evaluate(
        "id => !findBrowsePhoto(id)", burst_ids[0]
    ) is True

    page.evaluate("id => openExportModal([id])", burst_ids[0])
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    preview = page.locator("#exportPreview")
    # Resolved from the server: the hidden member's own filename.
    expect(preview).to_have_text("Preview: hawk1.jpg")
    # And never the cover's — that would name a file the export won't write.
    assert "hawk2" not in preview.inner_text()

    page.locator("#exportOverlay button", has_text="Cancel").click()


def test_color_label_chunk_failure_marks_nothing_fetched(live_server, page):
    """A failed chunk must never leave ids marked as definitively fetched.

    ``colorLabelsFetched`` is what lets the batch inspector read a missing
    entry as "no colour set" rather than "not asked yet", so a partly
    fetched id set must not be marked. Both failure shapes the transport can
    produce are covered: ``Vireo.api.json`` throws on any non-2xx (an
    ``{"error": ...}`` body is turned into a rejection, never returned), and
    resolves to null on an empty body.
    """
    page.goto(f"{live_server['url']}/browse")
    expect(page.locator("#grid > .grid-card")).to_have_count(5)

    outcome = page.evaluate(
        """async () => {
          var ids = [];
          for (var i = 1; i <= 501; i++) ids.push(i);
          var originalSafeFetch = safeFetch;
          var results = {};
          async function run(secondChunk) {
            colorLabels = {};
            colorLabelsFetched = new Set();
            safeFetch = function(url) {
              if (url.indexOf('/api/photos/color_labels') !== 0) {
                return originalSafeFetch.apply(this, arguments);
              }
              var match = /[?&]ids=([^&]*)/.exec(url);
              var first = match[1].split(',')[0];
              if (first === '1') {
                var ok = {};
                ok[first] = 'red';
                return Promise.resolve(ok);
              }
              return secondChunk();
            };
            try {
              await fetchColorLabels(ids);
            } catch (err) {
              // A rejected chunk propagates; the caller's follow-up is
              // skipped, which is the fail-closed outcome under test.
            } finally {
              safeFetch = originalSafeFetch;
            }
            return {
              fetchedFirst: colorLabelsFetched.has(1),
              fetchedLast: colorLabelsFetched.has(501),
              labelFirst: colorLabels['1'] === undefined ? null : colorLabels['1'],
              strayErrorKey: Object.prototype.hasOwnProperty.call(colorLabels, 'error'),
            };
          }
          results.rejected = await run(function() {
            // What the transport really does with a {"error": ...} body.
            var error = new Error('Internal server error');
            error.status = 500;
            error.body = {error: 'Internal server error'};
            return Promise.reject(error);
          });
          results.empty = await run(function() { return Promise.resolve(null); });
          return results;
        }"""
    )

    for shape in ("rejected", "empty"):
        # Neither the succeeded chunk nor the failed one is marked: the
        # inspector keeps saying "not loaded" instead of "no colour".
        assert outcome[shape]["fetchedFirst"] is False, shape
        assert outcome[shape]["fetchedLast"] is False, shape
        assert outcome[shape]["labelFirst"] is None, shape
        # And no error payload is merged in as if it were a photo's colour.
        assert outcome[shape]["strayErrorKey"] is False, shape


def test_export_preview_ignores_response_from_a_closed_modal(live_server, page):
    """A pending preview fetch must not repaint the next modal session.

    ``openExportModal`` drops ``_exportPreviewPhotos`` so a renamed photo
    cannot be previewed from a stale entry, but closing the modal does not
    cancel a request already in flight. Left unstamped, that response lands
    after the reopen, refills the cache that was just cleared, and shows the
    pre-rename filename the clearing existed to remove — and its surviving
    in-flight marker stops the new session from asking for itself
    (CodeRabbit on PR #1561).
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator("#grid > .grid-card")).to_have_count(3)

    outcome = page.evaluate(
        r"""async (photoId) => {
          var originalSafeFetch = safeFetch;
          var pending = [];
          safeFetch = function(url) {
            if (/^\/api\/photos\/\d+$/.test(url)) {
              var settle;
              var promise = new Promise(function(resolve) { settle = resolve; });
              pending.push({url: url, resolve: settle});
              return promise;
            }
            return originalSafeFetch.apply(this, arguments);
          };
          var tick = function() {
            return new Promise(function(resolve) { setTimeout(resolve, 0); });
          };
          var previewText = function() {
            return document.getElementById('exportPreview').textContent;
          };
          try {
            openExportModal([photoId]);
            await tick();
            var firstSessionFetches = pending.length;
            closeExportModal();
            openExportModal([photoId]);
            await tick();
            var fetchesAfterReopen = pending.length;
            // The first session's response finally arrives.
            pending[0].resolve({
              id: photoId, filename: 'renamed-before.jpg',
              timestamp: null, species: [],
            });
            await tick();
            await tick();
            var afterStale = previewText();
            // The reopened session's own request resolves normally.
            if (pending.length > 1) {
              pending[1].resolve({
                id: photoId, filename: 'hawk1.jpg',
                timestamp: null, species: [],
              });
              await tick();
              await tick();
            }
            return {
              firstSessionFetches: firstSessionFetches,
              fetchesAfterReopen: fetchesAfterReopen,
              afterStale: afterStale,
              afterFresh: previewText(),
            };
          } finally {
            safeFetch = originalSafeFetch;
            closeExportModal();
          }
        }""",
        burst_ids[0],
    )

    # The superseded response never reaches the preview...
    assert "renamed-before" not in outcome["afterStale"]
    assert "loading" in outcome["afterStale"]
    # ...and the current session's own response still does.
    assert outcome["afterFresh"] == "Preview: hawk1.jpg"
    assert outcome["firstSessionFetches"] == 1
    # The reopened modal asks for itself instead of adopting the orphan.
    assert outcome["fetchesAfterReopen"] == 2


def test_select_all_matching_puts_stack_cover_first_outside_collections(
    live_server, page
):
    """Select-all on the general query path must return cover-first order.

    ``/api/photos/query`` with ``ids_only`` returned raw member order, so the
    workspace, folder, dashboard-collection, unsaved-filter, and
    visual-search paths seeded ``selectedPhotos`` with a hidden burst frame
    whenever a stack's quality-ranked cover was not its earliest member.
    Best Batch, Burst Review, and the export preview all read that first
    entry, so they started on a photo the user could not see (Codex P2 on
    PR #1561).
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        # The middle (not the earliest) frame wins the cover ranking.
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator("#grid > .grid-card")).to_have_count(3)

    outcome = page.evaluate(
        """async () => {
          await selectAllMatchingPhotos();
          return {
            collectionScoped: !!(activeCollectionId && !dashboardCollectionScope),
            selected: Array.from(selectedPhotos),
            firstCardId: parseInt(
              document.querySelector('#grid > .grid-card').dataset.id, 10
            ),
          };
        }"""
    )
    # This test only means something on the non-collection path.
    assert outcome["collectionScoped"] is False
    # Every underlying photo is still selected — the projection reorders,
    # it never drops hidden members.
    assert sorted(outcome["selected"]) == sorted(live_server["data"]["photos"])
    # The first selected id is the visible first card, not a hidden frame.
    assert outcome["selected"][0] == outcome["firstCardId"] == burst_ids[1]


def test_stack_metadata_lookups_chunk_their_get_urls(live_server, page):
    """iNat and color-label lookups must chunk, like the by-ids expansion.

    An expanded stack hands every member id to ``loadInatStatus`` and
    ``fetchColorLabels``, which concatenate them into a GET query string. A
    stack large enough to exceed the request-target limit made both requests
    fail, silently dropping iNaturalist badges and color-label fields for
    the whole tray even though the chunked ``/api/photos/by-ids`` expansion
    succeeded (Codex P2 on PR #1561).
    """
    page.goto(f"{live_server['url']}/browse")
    expect(page.locator("#grid > .grid-card")).to_have_count(5)

    outcome = page.evaluate(
        """async () => {
          var ids = [];
          for (var i = 1; i <= 501; i++) ids.push(i);
          var originalSafeFetch = safeFetch;
          var inatChunks = [];
          var colorChunks = [];
          safeFetch = function(url) {
            var match = /[?&](?:photo_ids|ids)=([^&]*)/.exec(url);
            var count = match ? match[1].split(',').length : 0;
            if (url.indexOf('/api/inat/submissions') === 0) {
              inatChunks.push(count);
              var inat = {};
              inat[match[1].split(',')[0]] = true;
              return Promise.resolve(inat);
            }
            if (url.indexOf('/api/photos/color_labels') === 0) {
              colorChunks.push(count);
              var colors = {};
              colors[match[1].split(',')[0]] = 'red';
              return Promise.resolve(colors);
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            await loadInatStatus(ids);
            await fetchColorLabels(ids);
          } finally {
            safeFetch = originalSafeFetch;
          }
          return {
            inatChunks: inatChunks,
            colorChunks: colorChunks,
            // Results from every chunk have to survive the merge, not just
            // the last one.
            inatSubmitted: [inatSubmitted['1'], inatSubmitted['501']],
            colorLabels: [colorLabels['1'], colorLabels['501']],
            fetchedFirst: colorLabelsFetched.has(1),
            fetchedLast: colorLabelsFetched.has(501),
          };
        }"""
    )
    # Same 500-id cap the /api/photos/by-ids expansion path already uses.
    assert outcome["inatChunks"] == [500, 1]
    assert outcome["colorChunks"] == [500, 1]
    assert outcome["inatSubmitted"] == [True, True]
    assert outcome["colorLabels"] == ["red", "red"]
    assert outcome["fetchedFirst"] is True
    assert outcome["fetchedLast"] is True


def test_navbar_undo_restores_stack_cover_and_member_state(live_server, page):
    """Navbar undo of a cover-changing flag edit must reverse it on screen.

    The navbar's Undo button writes to the database and then announces
    ``vireo:edit-history-changed``; Browse's listener refreshed only the
    species/representative fields, so the reverted flag stayed in the cached
    member objects and the promoted photo stayed on top of the stack. The
    server had reversed the edit and the grid still showed the post-edit
    state — Undo looked like a no-op (Codex P2 on PR #1561).
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    # Flagging a hidden member outranks the quality-picked cover, so the
    # edit promotes it and caches the stack's members.
    assert page.evaluate("id => setFlagFor(id, 'flagged')", burst_ids[0]) is True
    expect(
        page.locator(f'.grid-card[data-id="{burst_ids[0]}"] .browse-stack-badge')
    ).to_be_visible()

    # The navbar Undo button: writes, then announces.
    page.evaluate("() => doUndo()")

    # The demoted cover comes back, because the flag that promoted the other
    # member is gone from the database.
    expect(
        page.locator(f'.grid-card[data-id="{burst_ids[1]}"] .browse-stack-badge')
    ).to_be_visible()
    expect(
        page.locator(f'.grid-card[data-id="{burst_ids[0]}"]')
    ).to_have_count(0)

    # ...and the cached member the undo reverted no longer claims to be
    # flagged, so a later reconciliation cannot re-promote it.
    page.wait_for_function(
        """id => {
          var member = findBrowsePhoto(id);
          return !!member && (member.flag == null || member.flag === 'none');
        }""",
        arg=burst_ids[0],
    )
    assert page.evaluate(
        """ids => {
          var members = browseStackMembers[String(ids[1])] || [];
          return members.map(function(m) { return m.id; }).sort(function(a, b) {
            return a - b;
          });
        }""",
        burst_ids,
    ) == sorted(burst_ids)
    # No silent staleness marker left behind by the reconciliation.
    assert page.evaluate(
        "ids => ids.some(id => browseStackCoverRecheck.has(id))", burst_ids
    ) is False

    # Ratings are the other reversible cover input and travel the same path:
    # an undo has to put the cached member's value back too, or the next
    # reconciliation ranks the stack on a rating the database no longer holds.
    before = page.evaluate("id => findBrowsePhoto(id).rating", burst_ids[2])
    page.evaluate("id => setRatingFor(id, 4)", burst_ids[2])
    page.wait_for_function(
        "id => findBrowsePhoto(id).rating === 4", arg=burst_ids[2]
    )
    page.evaluate("() => doUndo()")
    page.wait_for_function(
        """args => findBrowsePhoto(args[0]).rating === args[1]""",
        arg=[burst_ids[2], before],
    )


def test_keyword_edit_on_unloaded_members_invalidates_pending_expansion(
    live_server, page
):
    """A keyword edit that reaches only unloaded members must still invalidate.

    The tray offers "Select all" while it is still loading, so "Add to N" can
    touch members that exist in neither ``photos`` nor ``browseStackMembers``.
    ``_refreshBrowseKeywordState`` narrowed its ids to rows it could repaint
    before doing any invalidation, so when every touched member was unloaded it
    returned early and the pre-edit expansion response installed members
    without the species the user had just added (Codex P2 on PR #1561).
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    outcome = page.evaluate(
        """async ids => {
          var coverId = ids[1];
          var hiddenId = ids[0];
          var originalSafeFetch = safeFetch;
          var byIdsOptions = {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({photo_ids: ids}),
          };
          var preEdit = await originalSafeFetch('/api/photos/by-ids', byIdsOptions);
          var stalePayload = JSON.parse(JSON.stringify(preEdit));
          stalePayload.photos.forEach(function(photo) {
            photo.filename = 'PRE-EDIT.jpg';
          });
          var release = null;
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids' && !release) {
              return new Promise(function(resolve) { release = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            var expansion = toggleBrowseStack(null, coverId);
            while (!release) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            // The hidden member is loaded nowhere yet, so this call has no
            // local row to patch — but the edit still committed, and the
            // response in flight predates it.
            var loadedNowhere = findBrowsePhoto(hiddenId) === null;
            await _refreshBrowseKeywordState([hiddenId]);
            release(stalePayload);
            await expansion;
            return {
              loadedNowhere: loadedNowhere,
              filenames: (browseStackMembers[String(coverId)] || []).map(
                function(photo) { return photo.filename; }
              ).sort(),
              error: browseStackErrors[String(coverId)] || null,
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    assert outcome["loadedNowhere"] is True
    assert outcome["error"] is None
    assert "PRE-EDIT.jpg" not in outcome["filenames"]
    assert outcome["filenames"] == ["hawk1.jpg", "hawk2.jpg", "hawk3.jpg"]


def test_keyword_edit_on_unloaded_members_invalidates_pending_hydration(
    live_server, page
):
    """The same narrowing hid pre-edit rows from a pending cover hydration.

    ``reconcileBrowseStackCovers`` hydrates an uncached collapsed stack after a
    rating or flag edit. A keyword edit landing on one of that stack's members
    while the hydration is in flight has no local row to patch either, so the
    pre-edit payload used to be cached as the stack's members.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?",
            (burst_ids[1],),
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    expect(page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')).to_be_visible()

    outcome = page.evaluate(
        """async ids => {
          var coverId = ids[1];
          var hiddenId = ids[0];
          var originalSafeFetch = safeFetch;
          var byIdsOptions = {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({photo_ids: ids}),
          };
          var preEdit = await originalSafeFetch('/api/photos/by-ids', byIdsOptions);
          var stalePayload = JSON.parse(JSON.stringify(preEdit));
          stalePayload.photos.forEach(function(photo) {
            photo.filename = 'PRE-EDIT.jpg';
          });
          var release = null;
          safeFetch = function(url) {
            if (url === '/api/photos/by-ids' && !release) {
              return new Promise(function(resolve) { release = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            var hydration = reconcileBrowseStackCovers([hiddenId]);
            while (!release) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            var loadedNowhere = findBrowsePhoto(hiddenId) === null;
            await _refreshBrowseKeywordState([hiddenId]);
            release(stalePayload);
            await hydration;
            return {
              loadedNowhere: loadedNowhere,
              filenames: (browseStackMembers[String(coverId)] || []).map(
                function(photo) { return photo.filename; }
              ).sort(),
              recheck: browseStackCoverRecheck.has(coverId),
            };
          } finally {
            safeFetch = originalSafeFetch;
          }
        }""",
        burst_ids,
    )
    assert outcome["loadedNowhere"] is True
    assert "PRE-EDIT.jpg" not in outcome["filenames"]
    assert outcome["filenames"] == ["hawk1.jpg", "hawk2.jpg", "hawk3.jpg"]
    assert outcome["recheck"] is False
