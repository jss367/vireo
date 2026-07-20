(function () {
  "use strict";

  const STORAGE_KEY = "vireo-photo-filter-prototype-v1";
  const COLORS = ["red", "yellow", "green", "blue", "purple"];
  const FLAG_LABELS = { flagged: "Picked", none: "Unflagged", rejected: "Rejected" };
  const COLOR_LABELS = { red: "Red", yellow: "Yellow", green: "Green", blue: "Blue", purple: "Purple" };
  const CONTEXTS = {
    browse: {
      title: "Browse",
      description: "Explore every available photo in this workspace.",
      scope: "Workspace · All available photos",
    },
    map: {
      title: "Map",
      description: "See matching photos that have EXIF coordinates or a mapped location keyword.",
      scope: "Map · Plottable locations",
    },
    review: {
      title: "Prediction Review",
      description: "Review pending AI suggestions while refining by any photo metadata.",
      scope: "Review · Pending predictions",
    },
    duplicates: {
      title: "Duplicates",
      description: "A matching member reveals its complete duplicate group for a safe decision.",
      scope: "Duplicates · Grouped matches",
    },
  };

  const FIELD_DEFS = {
    all_text:       { label: "All searchable text", category: "Text & organization", type: "text", hint: "Filename, folder, keywords, camera, lens, location" },
    filename:       { label: "Filename", category: "File", type: "text" },
    folder:         { label: "Folder", category: "File", type: "text", suggest: true },
    extension:      { label: "File extension", category: "File", type: "enum", values: ["jpg", "cr3", "nef", "arw", "dng"] },
    file_size:      { label: "File size (MB)", category: "File", type: "number", step: "0.1" },
    width:          { label: "Width (px)", category: "File", type: "number", step: "1" },
    height:         { label: "Height (px)", category: "File", type: "number", step: "1" },
    date:           { label: "Capture date", category: "File", type: "date" },
    keyword:        { label: "Keyword", category: "Text & organization", type: "text", suggest: true },
    species:        { label: "Species", category: "Text & organization", type: "text", suggest: true },
    location:       { label: "Named location", category: "Location", type: "text", suggest: true },
    has_gps:        { label: "Has GPS", category: "Location", type: "boolean" },
    rating:         { label: "Rating", category: "Organization", type: "rating" },
    flag:           { label: "Flag", category: "Organization", type: "enum", values: ["flagged", "none", "rejected"], labels: FLAG_LABELS },
    color:          { label: "Color label", category: "Organization", type: "enum", values: COLORS, labels: COLOR_LABELS },
    camera_make:    { label: "Camera make", category: "Camera & exposure", type: "text", suggest: true },
    camera_model:   { label: "Camera model", category: "Camera & exposure", type: "text", suggest: true },
    lens:           { label: "Lens", category: "Camera & exposure", type: "text", suggest: true },
    focal_length:   { label: "Focal length (mm)", category: "Camera & exposure", type: "number", step: "1" },
    aperture:       { label: "Aperture", category: "Camera & exposure", type: "number", step: "0.1" },
    shutter:        { label: "Shutter speed (seconds)", category: "Camera & exposure", type: "number", step: "0.0001" },
    iso:            { label: "ISO", category: "Camera & exposure", type: "number", step: "1" },
    quality:        { label: "Quality score", category: "Quality & AI", type: "number", step: "0.01" },
    sharpness:      { label: "Sharpness", category: "Quality & AI", type: "number", step: "1" },
    prediction:     { label: "Predicted species", category: "This page · Review", type: "text" },
    confidence:     { label: "Prediction confidence", category: "This page · Review", type: "number", step: "0.01" },
    prediction_status: { label: "Prediction status", category: "This page · Review", type: "enum", values: ["pending", "accepted", "rejected"], labels: { pending: "Pending", accepted: "Accepted", rejected: "Rejected" } },
    duplicate_group: { label: "Duplicate group", category: "Workflow", type: "text" },
    edited:         { label: "Has edits", category: "Workflow", type: "boolean" },
    indexed:        { label: "Has visual index", category: "Workflow", type: "boolean" },
  };

  const OPS = {
    text: [
      ["contains", "contains"], ["not_contains", "does not contain"], ["eq", "is"], ["neq", "is not"],
      ["starts", "starts with"], ["ends", "ends with"], ["is_set", "is set"], ["not_set", "is not set"],
    ],
    number: [
      ["eq", "is"], ["neq", "is not"], ["gt", "is greater than"], ["gte", "is at least"],
      ["lt", "is less than"], ["lte", "is at most"], ["between", "is between"], ["is_set", "is set"], ["not_set", "is not set"],
    ],
    rating: [["eq", "is exactly"], ["neq", "is not"], ["gte", "is at least"], ["lte", "is at most"]],
    enum: [["in", "is one of"], ["not_in", "is not one of"], ["eq", "is"], ["neq", "is not"], ["is_set", "is set"], ["not_set", "is not set"]],
    boolean: [["eq", "is"]],
    date: [
      ["in_last", "is in the last"], ["eq", "is on"], ["gt", "is after"], ["gte", "is on or after"],
      ["lt", "is before"], ["lte", "is on or before"], ["between", "is between"], ["is_set", "is set"], ["not_set", "is not set"],
    ],
  };

  const DATE_UNITS = [["days", "day", "days"], ["weeks", "week", "weeks"], ["months", "month", "months"], ["years", "year", "years"]];
  const DATE_UNIT_DAYS = { days: 1, weeks: 7, months: 30.44, years: 365.25 };

  function todayString() {
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
  }

  function unitLabel(unit, n) {
    const entry = DATE_UNITS.find(([key]) => key === unit) || DATE_UNITS[0];
    return Number(n) === 1 ? entry[1] : entry[2];
  }

  const speciesProfiles = [
    ["Great Horned Owl", "owl", "forest", ["perched", "night", "portrait"], "#7a634e", "#c7a67b"],
    ["Barn Owl", "owl", "grassland", ["flying", "dusk", "wings"], "#9b6e45", "#f2d8a7"],
    ["Snowy Owl", "owl", "shore", ["perched", "snow", "portrait"], "#6c8798", "#edf5f7"],
    ["Bald Eagle", "eagle", "river", ["flying", "sky", "wings"], "#2d5262", "#d8c484"],
    ["Golden Eagle", "eagle", "mountains", ["flying", "dusk", "wings"], "#74522e", "#d3a85e"],
    ["American Robin", "songbird", "backyard", ["perched", "branch", "portrait"], "#557057", "#d78054"],
    ["Northern Cardinal", "songbird", "woodland", ["perched", "branch", "red"], "#584735", "#d8473f"],
    ["Anna's Hummingbird", "hummingbird", "garden", ["flying", "flowers", "wings"], "#28635d", "#bc4ca0"],
    ["Great Blue Heron", "wader", "wetlands", ["standing", "water", "portrait"], "#496b78", "#98b3bd"],
    ["Snowy Egret", "wader", "marsh", ["standing", "water", "white"], "#3b7181", "#e5eeee"],
    ["Peregrine Falcon", "raptor", "cliffs", ["flying", "sky", "fast"], "#47556c", "#aeb6c1"],
    ["Red-tailed Hawk", "raptor", "grassland", ["flying", "dusk", "wings"], "#6f5c3e", "#c58a5d"],
    ["Atlantic Puffin", "seabird", "coast", ["standing", "ocean", "colorful"], "#31586d", "#ee8b44"],
  ];
  const cameraProfiles = [
    ["Sony", "Sony α1", "FE 200-600mm F5.6-6.3 G OSS"],
    ["Canon", "Canon EOS R5", "RF 100-500mm F4.5-7.1 L IS USM"],
    ["Nikon", "Nikon Z9", "NIKKOR Z 600mm f/4 TC VR S"],
    ["Sony", "Sony α9 III", "FE 300mm F2.8 GM OSS"],
    ["Fujifilm", "Fujifilm X-H2S", "XF150-600mm F5.6-8 R LM OIS WR"],
  ];
  const folders = ["2026/Coastal Winter", "2026/Backyard Birds", "2025/Owls at Dusk", "2025/Wetlands", "2024/Raptor Migration", "2024/Archive Selects"];
  const locations = ["Bolsa Chica Wetlands", "Point Reyes", "Yosemite Valley", "Backyard", "Salton Sea", "Hawk Hill", null];

  function seeded(n, salt) {
    const x = Math.sin(n * 12.9898 + salt * 78.233) * 43758.5453;
    return x - Math.floor(x);
  }

  function svgThumb(photo) {
    const profile = speciesProfiles[photo.profileIndex];
    const title = escapeXml(photo.species.split(" ").slice(-1)[0]);
    const wing = photo.visualTags.includes("flying");
    const body = wing
      ? `<path d="M240 135c-43-56-102-63-151-30 48 0 73 27 95 54-38-8-69 5-91 35 55-17 96 0 147 30 51-30 92-47 147-30-22-30-53-43-91-35 22-27 47-54 95-54-49-33-108-26-151 30z" fill="${profile[5]}" opacity=".9"/><circle cx="240" cy="168" r="37" fill="${profile[4]}"/><path d="M272 168l38 13-38 10z" fill="#e7b349"/>`
      : `<ellipse cx="240" cy="180" rx="61" ry="79" fill="${profile[5]}"/><circle cx="240" cy="116" r="53" fill="${profile[4]}"/><circle cx="221" cy="108" r="9" fill="#f4d56c"/><circle cx="259" cy="108" r="9" fill="#f4d56c"/><circle cx="221" cy="108" r="4" fill="#111"/><circle cx="259" cy="108" r="4" fill="#111"/><path d="M234 126l12 0-6 12z" fill="#e7b349"/><path d="M213 251v29m54-29v29" stroke="#d8b574" stroke-width="7"/>`;
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 480 360"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="${profile[4]}"/><stop offset="1" stop-color="#102a38"/></linearGradient><filter id="b"><feGaussianBlur stdDeviation="18"/></filter></defs><rect width="480" height="360" fill="url(#g)"/><circle cx="74" cy="75" r="86" fill="${profile[5]}" opacity=".2" filter="url(#b)"/><circle cx="400" cy="265" r="108" fill="#061923" opacity=".38" filter="url(#b)"/>${body}<text x="18" y="335" fill="white" opacity=".7" font-family="sans-serif" font-size="16" letter-spacing="2">${title.toUpperCase()}</text></svg>`;
    return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
  }

  function escapeXml(value) {
    return String(value).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;" }[c]));
  }

  function createPhotos() {
    const photos = [];
    for (let i = 1; i <= 54; i += 1) {
      const profileIndex = (i * 7 + Math.floor(i / 4)) % speciesProfiles.length;
      const profile = speciesProfiles[profileIndex];
      const camera = cameraProfiles[(i * 3 + 1) % cameraProfiles.length];
      const juvenile = i % 9 === 0;
      const extension = ["cr3", "arw", "nef", "jpg", "dng"][i % 5];
      const gps = i % 6 !== 0;
      const duplicateGroup = i >= 37 && i <= 48 ? `DG-${Math.floor((i - 37) / 3) + 1}` : null;
      const rating = (i * 3 + profileIndex) % 6;
      const flag = i % 11 === 0 ? "rejected" : (i % 4 === 0 || rating === 5 ? "flagged" : "none");
      const color = i % 7 === 0 ? null : COLORS[(i + profileIndex) % COLORS.length];
      const iso = [100, 200, 400, 800, 1600, 3200, 6400][i % 7];
      const year = 2024 + (i % 3);
      const month = ((i * 5) % 12) + 1;
      const day = ((i * 7) % 27) + 1;
      const location = gps ? locations[i % (locations.length - 1)] : (i % 12 === 0 ? locations[(i + 2) % 6] : null);
      const visualTags = profile[3].concat([profile[1], profile[2], i % 2 ? "wildlife" : "bird", i % 5 === 0 ? "backlit" : "natural light"]);
      const photo = {
        id: i,
        profileIndex,
        species: profile[0],
        filename: `${profile[1]}_${juvenile ? "juvenile_" : ""}${String(2100 + i).padStart(4, "0")}.${extension}`,
        extension,
        folder: folders[(i + profileIndex) % folders.length],
        keywords: [profile[1], profile[2], juvenile ? "juvenile" : "adult", i % 2 ? "wildlife" : "bird"],
        rating,
        flag,
        color,
        camera_make: camera[0],
        camera_model: camera[1],
        lens: camera[2],
        focal_length: [300, 400, 500, 600, 840][i % 5],
        aperture: [2.8, 4, 5.6, 6.3, 8][i % 5],
        shutter: [1 / 4000, 1 / 2500, 1 / 1600, 1 / 800, 1 / 320][i % 5],
        iso,
        width: i % 2 ? 8640 : 8192,
        height: i % 2 ? 5760 : 5464,
        file_size: Math.round((18 + seeded(i, 3) * 55) * 10) / 10,
        date: `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
        location,
        has_gps: gps,
        plottable: Boolean(gps || location),
        quality: Math.round((0.43 + seeded(i, 7) * 0.55) * 100) / 100,
        sharpness: Math.round(120 + seeded(i, 9) * 1280),
        prediction: i % 8 === 0 ? "Uncertain raptor" : profile[0],
        prediction_status: i % 5 === 0 ? "accepted" : (i % 13 === 0 ? "rejected" : "pending"),
        confidence: Math.round((0.54 + seeded(i, 11) * 0.45) * 100) / 100,
        duplicate_group: duplicateGroup,
        edited: i % 3 === 0,
        indexed: i % 10 !== 0,
        visualTags,
      };
      photo.thumbnail = svgThumb(photo);
      photos.push(photo);
    }
    return photos;
  }

  const photos = createPhotos();

  function emptyPageState() {
    return { root: { kind: "group", match: "all", children: [] }, visual: null, advanced: false, muted: false };
  }

  function defaultState() {
    return {
      context: "browse",
      pages: { browse: emptyPageState(), map: emptyPageState(), review: emptyPageState(), duplicates: emptyPageState() },
      view: { sort: "date_desc", thumbSize: 200, showDetails: true },
      theme: "vireo-dark",
      checklist: {},
      savedCollections: [],
    };
  }

  function loadState() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return defaultState();
      const parsed = JSON.parse(raw);
      const fallback = defaultState();
      return {
        ...fallback,
        ...parsed,
        pages: { ...fallback.pages, ...(parsed.pages || {}) },
        view: { ...fallback.view, ...(parsed.view || {}) },
      };
    } catch (_error) {
      return defaultState();
    }
  }

  let state = loadState();
  let lastSnapshot = null;
  let toastTimer = null;
  let queryTimer = null;
  let clipTimer = null;
  let searchIndex = -1;

  const $ = (id) => document.getElementById(id);
  const pageState = () => state.pages[state.context];
  const clone = (value) => JSON.parse(JSON.stringify(value));

  function persist() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }

  function snapshot() {
    lastSnapshot = clone(state);
  }

  function mutate(fn, options) {
    const opts = options || {};
    if (!opts.noSnapshot) snapshot();
    fn();
    persist();
    scheduleRender(opts.clip ? 620 : 140, opts.message);
  }

  function scheduleRender(delay, message) {
    clearTimeout(queryTimer);
    $("queryStatusText").textContent = message || "Updating results…";
    $("queryStatus").hidden = false;
    queryTimer = setTimeout(() => {
      $("queryStatus").hidden = true;
      render();
    }, delay == null ? 120 : delay);
  }

  function showToast(text, action) {
    clearTimeout(toastTimer);
    $("toastText").textContent = text;
    $("toastAction").hidden = !action;
    $("toastAction").onclick = action || null;
    $("toast").hidden = false;
    toastTimer = setTimeout(() => { $("toast").hidden = true; }, 4200);
  }

  function showUndoToast(text) {
    const undoState = clone(lastSnapshot);
    showToast(text, undoState ? () => {
      state = undoState;
      persist();
      render();
      $("toast").hidden = true;
    } : null);
  }

  function fieldValue(photo, field) {
    if (field === "all_text") {
      return [photo.filename, photo.folder, photo.species, photo.location, photo.camera_make, photo.camera_model, photo.lens, photo.extension].concat(photo.keywords).filter(Boolean).join(" ");
    }
    if (field === "keyword") return photo.keywords;
    if (field === "species") return photo.species;
    if (field === "color") return photo.color;
    if (field === "date") return photo.date;
    return photo[field];
  }

  function isMissing(value) {
    return value == null || value === "" || (Array.isArray(value) && value.length === 0);
  }

  function normalizeText(value, caseSensitive) {
    const text = String(value == null ? "" : value);
    return caseSensitive ? text : text.toLocaleLowerCase();
  }

  function evaluateLeaf(photo, rule) {
    const value = fieldValue(photo, rule.field);
    const op = rule.op;
    if (op === "is_set") return !isMissing(value);
    if (op === "not_set") return isMissing(value);

    const def = FIELD_DEFS[rule.field] || FIELD_DEFS.all_text;
    if (def.type === "text") {
      const wanted = normalizeText(rule.value, rule.caseSensitive);
      const values = Array.isArray(value) ? value : [value];
      const normalized = values.filter((v) => !isMissing(v)).map((v) => normalizeText(v, rule.caseSensitive));
      const positive = normalized.some((actual) => {
        if (op === "contains" || op === "not_contains") return actual.includes(wanted);
        if (op === "eq" || op === "neq") return actual === wanted;
        if (op === "starts") return actual.startsWith(wanted);
        if (op === "ends") return actual.endsWith(wanted);
        return false;
      });
      if (op === "not_contains" || op === "neq") return !positive;
      return positive;
    }

    if (def.type === "boolean") return Boolean(value) === Boolean(rule.value);
    if (def.type === "enum") {
      const wantedList = Array.isArray(rule.value) ? rule.value : [rule.value];
      if (op === "in") return !isMissing(value) && wantedList.includes(value);
      if (op === "not_in") return isMissing(value) || !wantedList.includes(value);
      if (op === "neq") return isMissing(value) || value !== rule.value;
      return value === rule.value;
    }
    if (def.type === "date") {
      if (isMissing(value)) return op === "neq";
      const actual = String(value);
      if (op === "in_last") {
        const spec = rule.value || {};
        const days = DATE_UNIT_DAYS[spec.unit || "days"] * Number(spec.n || 0);
        const cutoff = new Date(Date.now() - days * 86400000);
        const captured = new Date(`${actual}T12:00:00`);
        return captured >= cutoff && captured <= new Date(Date.now() + 86400000);
      }
      if (op === "between") {
        const spec = rule.value || {};
        return actual >= String(spec.from || "") && actual <= String(spec.to || "");
      }
      const wanted = String(rule.value || "");
      if (op === "eq") return actual === wanted;
      if (op === "gt") return actual > wanted;
      if (op === "gte") return actual >= wanted;
      if (op === "lt") return actual < wanted;
      if (op === "lte") return actual <= wanted;
      return false;
    }

    if (isMissing(value)) return op === "neq";
    const actual = Number(value);
    if (op === "between") {
      const spec = rule.value || {};
      return actual >= Number(spec.from) && actual <= Number(spec.to);
    }
    const wanted = Number(rule.value);
    if (op === "eq") return actual === wanted;
    if (op === "neq") return actual !== wanted;
    if (op === "gt") return actual > wanted;
    if (op === "gte") return actual >= wanted;
    if (op === "lt") return actual < wanted;
    if (op === "lte") return actual <= wanted;
    return false;
  }

  function evaluateNode(photo, node) {
    if (!node) return true;
    if (node.kind !== "group") return evaluateLeaf(photo, node);
    if (!node.children.length) return true;
    const values = node.children.map((child) => evaluateNode(photo, child));
    if (node.match === "any") return values.some(Boolean);
    if (node.match === "none") return !values.some(Boolean);
    return values.every(Boolean);
  }

  function evaluateNodeExcept(photo, node, skip) {
    if (!node || node === skip) return true;
    if (node.kind !== "group") return evaluateLeaf(photo, node);
    // Drop the skipped clause entirely instead of treating it as true, otherwise
    // an "any" group short-circuits to true and a "none" group short-circuits to
    // false, and facet counts stop respecting the sibling rules.
    const remaining = node.children.filter((child) => child !== skip);
    if (!remaining.length) return true;
    const values = remaining.map((child) => evaluateNodeExcept(photo, child, skip));
    if (node.match === "any") return values.some(Boolean);
    if (node.match === "none") return !values.some(Boolean);
    return values.every(Boolean);
  }

  function matchesExceptRule(photo, skip) {
    const page = pageState();
    return contextMatches(photo, state.context)
      && evaluateNodeExcept(photo, page.root, skip)
      && matchesVisual(photo, page);
  }

  function contextMatches(photo, context) {
    if (context === "map") return photo.plottable;
    if (context === "review") return photo.prediction_status === "pending";
    if (context === "duplicates") return Boolean(photo.duplicate_group);
    return true;
  }

  const VISUAL_THRESHOLDS = { broad: .29, balanced: .43, strict: .58 };

  function visualIsActive(visual) {
    return Boolean(visual) && !["unsupported", "no_index"].includes(visual.status);
  }

  function matchesVisual(photo, page) {
    if (!visualIsActive(page.visual)) return true;
    const threshold = VISUAL_THRESHOLDS[page.visual.strength || "balanced"];
    return Boolean(photo.indexed) && visualScore(photo, page.visual.prompt) >= threshold;
  }

  function visualScore(photo, prompt) {
    const tokens = String(prompt || "").toLocaleLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
    const haystack = [photo.species, photo.filename].concat(photo.visualTags).join(" ").toLocaleLowerCase();
    const aliases = { bird: ["owl", "eagle", "raptor", "songbird", "wader", "seabird", "hummingbird"], flight: ["flying", "wings"], sunset: ["dusk", "backlit"], fly: ["flying"] };
    let hits = 0;
    tokens.forEach((token) => {
      if (haystack.includes(token)) hits += 1;
      else if ((aliases[token] || []).some((alias) => haystack.includes(alias))) hits += .72;
    });
    const semantic = tokens.length ? hits / tokens.length : 0;
    return Math.min(.98, .16 + semantic * .72 + seeded(photo.id, prompt.length + 5) * .13);
  }

  function applyUserFilters(page) {
    let result = photos.filter((photo) => contextMatches(photo, state.context) && evaluateNode(photo, page.root));
    if (visualIsActive(page.visual)) {
      const threshold = VISUAL_THRESHOLDS[page.visual.strength || "balanced"];
      result = result
        .filter((photo) => photo.indexed && visualScore(photo, page.visual.prompt) >= threshold)
        .map((photo) => ({ ...photo, _similarity: visualScore(photo, page.visual.prompt) }));
    }
    return result;
  }

  function getFilteredPhotos() {
    const page = pageState();
    if (page.muted) return sortPhotos(photos.filter((photo) => contextMatches(photo, state.context)));
    return sortPhotos(applyUserFilters(page));
  }

  function sortPhotos(items) {
    const result = items.slice();
    const sort = state.view.sort;
    const page = pageState();
    // Only rank by visualScore when the visual clause is actually being applied by
    // getFilteredPhotos(). Otherwise the "metadata filters shown only" error badge
    // (unsupported / missing index) or the paused-filters state would still let the
    // disabled prompt influence result order.
    const visualForSort = !page.muted && visualIsActive(page.visual) ? page.visual : null;
    if (sort === "relevance" && visualForSort) result.sort((a, b) => (b._similarity || visualScore(b, visualForSort.prompt)) - (a._similarity || visualScore(a, visualForSort.prompt)));
    else if (sort === "date_asc") result.sort((a, b) => a.date.localeCompare(b.date) || a.id - b.id);
    else if (sort === "name_asc") result.sort((a, b) => a.filename.localeCompare(b.filename));
    else if (sort === "rating_desc") result.sort((a, b) => b.rating - a.rating || b.quality - a.quality);
    else result.sort((a, b) => b.date.localeCompare(a.date) || b.id - a.id);
    return result;
  }

  function allLeaves(node, result) {
    const target = result || [];
    if (!node) return target;
    if (node.kind === "group") node.children.forEach((child) => allLeaves(child, target));
    else target.push(node);
    return target;
  }

  function getRuleAtPath(path) {
    if (path === "root" || path === "") return pageState().root;
    const parts = String(path).split(".").map(Number);
    let node = pageState().root;
    parts.forEach((index) => { node = node.children[index]; });
    return node;
  }

  function getParentAtPath(path) {
    const parts = String(path).split(".").map(Number);
    const index = parts.pop();
    let parent = pageState().root;
    parts.forEach((part) => { parent = parent.children[part]; });
    return { parent, index };
  }

  function defaultOperator(def) {
    if (def.type === "text") return "contains";
    if (def.type === "boolean") return "eq";
    if (def.type === "rating") return "gte";
    if (def.type === "enum") return "in";
    if (def.type === "date") return "in_last";
    return "eq";
  }

  function defaultValue(field, op) {
    const def = FIELD_DEFS[field];
    if (op === "in" || op === "not_in") return [def.values[0]];
    if (op === "in_last") return { n: 12, unit: "months" };
    if (op === "between") {
      if (def.type === "date") return { from: "2025-01-01", to: todayString() };
      const base = Number(defaultValue(field, "eq"));
      return { from: base, to: base * 2 };
    }
    if (def.type === "boolean") return true;
    if (def.type === "enum") return def.values[0];
    if (def.type === "rating") return 3;
    if (def.type === "number") return field === "confidence" || field === "quality" ? .8 : (field === "iso" ? 1600 : 400);
    if (def.type === "date") return "2025-01-01";
    return "";
  }

  function coerceValue(field, op, previous) {
    const def = FIELD_DEFS[field];
    if (op === "in" || op === "not_in") {
      if (Array.isArray(previous)) return previous.length ? previous : [def.values[0]];
      return def.values.includes(previous) ? [previous] : [def.values[0]];
    }
    if (op === "in_last") {
      return previous && typeof previous === "object" && previous.n != null ? previous : defaultValue(field, op);
    }
    if (op === "between") {
      if (previous && typeof previous === "object" && previous.from != null) return previous;
      const scalar = typeof previous === "object" ? null : previous;
      if (def.type === "date") return { from: typeof scalar === "string" && scalar ? scalar : "2025-01-01", to: todayString() };
      const base = Number(scalar);
      return Number.isFinite(base) && base !== 0 ? { from: base, to: base * 2 } : defaultValue(field, op);
    }
    if (Array.isArray(previous)) return previous[0] != null ? previous[0] : defaultValue(field, op);
    if (previous && typeof previous === "object") return defaultValue(field, op);
    if (def.type === "enum" && !def.values.includes(previous)) return def.values[0];
    return previous == null || previous === "" ? defaultValue(field, op) : previous;
  }

  function makeRule(field, op, value) {
    const def = FIELD_DEFS[field];
    const resolvedOp = op || defaultOperator(def);
    return { kind: "rule", field, op: resolvedOp, value: value == null ? defaultValue(field, resolvedOp) : value, caseSensitive: false };
  }

  function findRootRule(field) {
    return pageState().root.children.find((rule) => rule.kind === "rule" && rule.field === field);
  }

  function setQuickRule(field, op, value) {
    mutate(() => {
      const children = pageState().root.children;
      const index = children.findIndex((rule) => rule.kind === "rule" && rule.field === field);
      if (index >= 0 && children[index].op === op && children[index].value === value) children.splice(index, 1);
      else if (index >= 0) children[index] = makeRule(field, op, value);
      else children.unshift(makeRule(field, op, value));
    });
  }

  function toggleQuickEnum(field, value) {
    mutate(() => {
      const children = pageState().root.children;
      const index = children.findIndex((rule) => rule.kind === "rule" && rule.field === field);
      if (index < 0) { children.unshift(makeRule(field, "in", [value])); return; }
      const rule = children[index];
      let values;
      if (rule.op === "in" && Array.isArray(rule.value)) values = rule.value.slice();
      else if (rule.op === "eq") values = [rule.value];
      else { children[index] = makeRule(field, "in", [value]); return; }
      if (values.includes(value)) values = values.filter((entry) => entry !== value);
      else values.push(value);
      if (!values.length) children.splice(index, 1);
      else children[index] = makeRule(field, "in", values);
    });
  }

  function quickEnumValues(field) {
    const rule = findRootRule(field);
    if (!rule) return [];
    if (rule.op === "in" && Array.isArray(rule.value)) return rule.value;
    if (rule.op === "eq") return [rule.value];
    return [];
  }

  function ruleLabel(rule) {
    const def = FIELD_DEFS[rule.field] || { label: rule.field, type: "text" };
    const opLabel = (OPS[def.type] || []).find(([key]) => key === rule.op)?.[1] || rule.op;
    let value = rule.value;
    if (["in", "not_in"].includes(rule.op)) {
      value = (Array.isArray(rule.value) ? rule.value : [rule.value]).map((entry) => def.labels?.[entry] || entry).join(", ") || "(none)";
    } else if (rule.op === "in_last") {
      const spec = rule.value || {};
      value = `${spec.n} ${unitLabel(spec.unit, spec.n)}`;
    } else if (rule.op === "between") {
      const spec = rule.value || {};
      value = `${spec.from} and ${spec.to}`;
    } else {
      if (def.labels) value = def.labels[value] || value;
      if (def.type === "boolean") value = rule.value ? "Yes" : "No";
      if (rule.field === "rating") value = `${rule.value} ${Number(rule.value) === 1 ? "star" : "stars"}`;
    }
    if (["is_set", "not_set"].includes(rule.op)) return `${def.label} ${opLabel}`;
    return `${def.label} ${opLabel} ${value}`;
  }

  function expressionSummary() {
    const page = pageState();
    const parts = [];
    if (page.visual) parts.push(`Visually similar to “${page.visual.prompt}” (${page.visual.strength || "balanced"})`);
    function summarizeNode(node) {
      if (node.kind !== "group") return ruleLabel(node);
      if (!node.children.length) return "";
      // "none" means NONE of the children match (¬A ∧ ¬B), which is equivalent
      // to NOT (A OR B) — so join siblings with OR before wrapping in NOT,
      // otherwise NOT (A AND B) reads as "not both", which is much broader.
      const joiner = node.match === "all" ? " AND " : " OR ";
      const body = node.children.map(summarizeNode).filter(Boolean).join(joiner);
      return node.match === "none" ? `NOT (${body})` : (node === page.root ? body : `(${body})`);
    }
    const rules = summarizeNode(page.root);
    if (rules) parts.push(rules);
    return parts.join(" AND ") || "No user filters";
  }

  function escapeHtml(value) {
    // textContent → innerHTML only escapes &, <, >; quotes stay literal and would
    // break the double-quoted attributes in chip, rule, and suggestion templates
    // if a user-supplied value ever contained one.
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function render() {
    document.documentElement.dataset.theme = state.theme;
    syncQuickSearchInput();
    const context = CONTEXTS[state.context];
    $("pageTitle").textContent = context.title;
    $("pageDescription").textContent = context.description;
    $("scopeChipText").textContent = context.scope;
    document.querySelectorAll(".context-tab").forEach((button) => {
      const active = button.dataset.context === state.context;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", String(active));
    });

    const result = getFilteredPhotos();
    const hasVisualError = pageState().visual && ["unsupported", "no_index"].includes(pageState().visual.status);
    $("resultCount").textContent = result.length.toLocaleString();
    $("resultLabel").textContent = state.context === "duplicates" ? "matching photos" : (result.length === 1 ? "photo" : "photos");
    $("popoverMatchCount").textContent = `${result.length} matching ${result.length === 1 ? "photo" : "photos"}`;
    $("clearAll").hidden = !hasUserFilters();
    const page = pageState();
    $("muteFilters").hidden = !hasUserFilters() && !page.muted;
    $("muteFilters").textContent = page.muted ? "▶ Resume" : "⏸ Pause";
    $("muteFilters").classList.toggle("active", page.muted);
    $("filterSecondaryRow").classList.toggle("muted", page.muted);
    renderChips();
    renderQuickFilters();
    renderRules();
    renderContent(result, hasVisualError);
    renderBadges(result, hasVisualError);
    renderPresets();
    updateViewControls();
    requestAnimationFrame(updateChipOverflow);
  }

  function renderChips() {
    const page = pageState();
    const entries = [];
    if (page.visual) entries.push({ type: "visual", value: page.visual, label: `Visually similar · ${page.visual.prompt}` });
    allLeaves(page.root).forEach((rule) => entries.push({ type: "rule", value: rule, label: ruleLabel(rule) }));
    $("chipList").innerHTML = entries.map((entry, index) => {
      const error = entry.type === "visual" && ["unsupported", "no_index"].includes(entry.value.status);
      return `<button class="filter-chip ${entry.type === "visual" ? "visual" : ""} ${error ? "error" : ""}" data-chip-index="${index}" data-chip-type="${entry.type}" type="button" title="Edit ${escapeHtml(entry.label)}">
        <span>${entry.type === "visual" ? "✦ " : ""}${escapeHtml(entry.label)}</span>
        <span class="chip-remove" data-remove-chip="${index}" aria-label="Remove ${escapeHtml(entry.label)}">×</span>
      </button>`;
    }).join("");
    $("filterButtonCount").textContent = entries.length;
    $("filterButtonCount").hidden = entries.length === 0;
    $("filterButton").classList.toggle("has-filters", entries.length > 0);
  }

  function updateChipOverflow() {
    const viewport = $("chipViewport");
    const chips = Array.from($("chipList").children);
    chips.forEach((chip) => { chip.style.display = "inline-flex"; });
    $("overflowChip").hidden = true;
    if (!chips.length || viewport.clientWidth <= 0) return;
    let used = 0;
    let hidden = 0;
    const reserve = 44;
    chips.forEach((chip) => {
      const width = chip.getBoundingClientRect().width + 6;
      if (used + width > Math.max(20, viewport.clientWidth - reserve)) {
        chip.style.display = "none";
        hidden += 1;
      } else used += width;
    });
    if (hidden) {
      $("overflowChip").textContent = `+${hidden}`;
      $("overflowChip").hidden = false;
    }
  }

  function renderQuickFilters() {
    const rating = findRootRule("rating");
    if (rating && ["gte", "eq", "lte"].includes(rating.op)) $("quickRatingOp").value = rating.op;
    document.querySelectorAll("#quickStars button").forEach((button) => {
      const value = Number(button.dataset.rating);
      button.classList.toggle("active", Boolean(rating && Number(rating.value) >= value));
      const op = $("quickRatingOp").value;
      const comparison = op === "eq" ? "Exactly" : (op === "lte" ? "At most" : "At least");
      button.setAttribute("aria-label", `${comparison} ${value} ${value === 1 ? "star" : "stars"}`);
    });
    const flags = quickEnumValues("flag");
    document.querySelectorAll("#quickFlags button").forEach((button) => button.classList.toggle("active", flags.includes(button.dataset.flag)));
    const colors = quickEnumValues("color");
    document.querySelectorAll("#quickColors button").forEach((button) => button.classList.toggle("active", colors.includes(button.dataset.color)));
  }

  function renderRules() {
    const page = pageState();
    $("advancedToggle").checked = page.advanced;
    $("addGroupButton").hidden = !page.advanced;
    $("logicSummary").innerHTML = `Match <strong>${page.root.match}</strong> of these rules`;
    const active = document.activeElement;
    const restore = active && $("ruleTree").contains(active) && active.dataset && active.dataset.path != null
      ? { action: active.dataset.action, path: active.dataset.path, start: active.selectionStart, end: active.selectionEnd, suggest: Boolean(active.dataset.suggest) }
      : null;
    $("ruleTree").innerHTML = page.root.children.length
      ? page.root.children.map((node, index) => renderRuleNode(node, String(index), 0)).join("")
      : `<div style="padding:12px;border:1px dashed var(--border-primary);border-radius:7px;color:var(--text-dim);font-size:10px;text-align:center;">No rules yet. Use a quick filter or add any metadata field.</div>`;
    if (restore) {
      const el = $("ruleTree").querySelector(`[data-action="${restore.action}"][data-path="${restore.path}"]`);
      if (el) {
        el.focus({ preventScroll: true });
        if (restore.start != null) { try { el.setSelectionRange(restore.start, restore.end); } catch (_error) { /* selects don't support ranges */ } }
        if (restore.suggest) showValueSuggest(el);
      }
    }
  }

  function renderRuleNode(node, path, depth) {
    if (node.kind === "group") {
      return `<div class="rule-group" data-depth="${depth}">
        <div class="group-mode"><span>Match</span>
          <select data-action="match" data-path="${path}" aria-label="Group logic">
            <option value="all" ${node.match === "all" ? "selected" : ""}>all</option>
            <option value="any" ${node.match === "any" ? "selected" : ""}>any</option>
            <option value="none" ${node.match === "none" ? "selected" : ""}>none</option>
          </select><span>in this group</span>
          <button class="remove-rule" data-action="remove" data-path="${path}" type="button" aria-label="Remove group">×</button>
        </div>
        <div class="group-children">${node.children.map((child, index) => renderRuleNode(child, `${path}.${index}`, depth + 1)).join("")}</div>
        <div class="group-actions"><button class="text-button" data-action="add-child" data-path="${path}" type="button">＋ Rule</button><button class="text-button" data-action="add-subgroup" data-path="${path}" type="button">＋ Group</button></div>
      </div>`;
    }
    const def = FIELD_DEFS[node.field] || FIELD_DEFS.all_text;
    const fieldOptions = Object.entries(FIELD_DEFS).map(([key, value]) => `<option value="${key}" ${key === node.field ? "selected" : ""}>${escapeHtml(value.label)}</option>`).join("");
    const opOptions = (OPS[def.type] || OPS.text).map(([key, label]) => `<option value="${key}" ${key === node.op ? "selected" : ""}>${label}</option>`).join("");
    const noValue = ["is_set", "not_set"].includes(node.op);
    return `<div class="rule-row">
      <select data-action="field" data-path="${path}" aria-label="Filter field">${fieldOptions}</select>
      <select data-action="op" data-path="${path}" aria-label="Filter operator">${opOptions}</select>
      ${noValue ? `<span style="color:var(--text-dim);font-size:10px;padding-left:7px;">No value needed</span>` : renderRuleInput(node, def, path)}
      <button class="remove-rule" data-action="remove" data-path="${path}" type="button" aria-label="Remove ${escapeHtml(def.label)} rule">×</button>
      ${def.type === "text" && !noValue ? `<div class="rule-options"><label><input data-action="case" data-path="${path}" type="checkbox" ${node.caseSensitive ? "checked" : ""}> Match case</label></div>` : ""}
    </div>`;
  }

  function renderRuleInput(node, def, path) {
    if (["in", "not_in"].includes(node.op) && def.type === "enum") {
      const selected = Array.isArray(node.value) ? node.value : [node.value];
      return `<div class="enum-multi" role="group" aria-label="Filter values">${def.values.map((value) => {
        const count = photos.filter((photo) => matchesExceptRule(photo, node) && fieldValue(photo, node.field) === value).length;
        return `<button type="button" class="enum-toggle ${selected.includes(value) ? "active" : ""}" data-action="multi-toggle" data-path="${path}" data-value="${escapeHtml(value)}" aria-pressed="${selected.includes(value)}">${escapeHtml(def.labels?.[value] || value)}<em>${count}</em></button>`;
      }).join("")}</div>`;
    }
    if (node.op === "in_last") {
      const spec = node.value || {};
      return `<div class="value-pair"><input data-action="rel-n" data-path="${path}" type="number" min="1" step="1" value="${escapeHtml(spec.n)}" aria-label="Number of units"><select data-action="rel-unit" data-path="${path}" aria-label="Unit">${DATE_UNITS.map(([key, , plural]) => `<option value="${key}" ${spec.unit === key ? "selected" : ""}>${plural}</option>`).join("")}</select></div>`;
    }
    if (node.op === "between") {
      const spec = node.value || {};
      const type = def.type === "date" ? "date" : "number";
      return `<div class="value-pair"><input data-action="value-from" data-path="${path}" type="${type}" step="${def.step || 1}" value="${escapeHtml(spec.from)}" aria-label="Lower bound"><span>and</span><input data-action="value-to" data-path="${path}" type="${type}" step="${def.step || 1}" value="${escapeHtml(spec.to)}" aria-label="Upper bound"></div>`;
    }
    if (def.type === "enum") {
      return `<select data-action="value" data-path="${path}" aria-label="Filter value">${def.values.map((value) => `<option value="${escapeHtml(value)}" ${value === node.value ? "selected" : ""}>${escapeHtml(def.labels?.[value] || value)}</option>`).join("")}</select>`;
    }
    if (def.type === "boolean") {
      return `<select data-action="boolean" data-path="${path}" aria-label="Filter value"><option value="true" ${node.value === true ? "selected" : ""}>Yes</option><option value="false" ${node.value === false ? "selected" : ""}>No</option></select>`;
    }
    const type = def.type === "number" || def.type === "rating" ? "number" : (def.type === "date" ? "date" : "text");
    const max = def.type === "rating" ? `max="5" min="0"` : "";
    if (def.suggest && type === "text") {
      return `<span class="value-wrap"><input data-action="value-input" data-path="${path}" data-suggest="1" type="text" autocomplete="off" spellcheck="false" value="${escapeHtml(node.value)}" aria-label="Filter value" placeholder="Type or pick a value…"><div class="value-suggest" hidden></div></span>`;
    }
    return `<input data-action="value-input" data-path="${path}" type="${type}" ${max} step="${def.step || 1}" value="${escapeHtml(node.value)}" aria-label="Filter value">`;
  }

  function renderContent(result, hasVisualError) {
    $("photoGrid").hidden = state.context !== "browse";
    $("mapView").hidden = state.context !== "map";
    $("reviewView").hidden = state.context !== "review";
    $("duplicatesView").hidden = state.context !== "duplicates";
    $("emptyState").hidden = result.length > 0 || hasVisualError;
    if (state.context === "browse") renderPhotoGrid(result);
    else if (state.context === "map") renderMap(result);
    else if (state.context === "review") renderReview(result);
    else renderDuplicates(result);
  }

  function cardHtml(photo) {
    const stars = photo.rating ? "★".repeat(photo.rating) : "Unrated";
    const color = photo.color ? `<span class="photo-color" style="--label-color:${labelColor(photo.color)}" title="${COLOR_LABELS[photo.color]}"></span>` : "";
    const flag = photo.flag !== "none" ? `<span class="photo-flag ${photo.flag}" title="${FLAG_LABELS[photo.flag]}">${photo.flag === "flagged" ? "⚑" : "×"}</span>` : "";
    const similarity = photo._similarity != null ? `<span class="photo-similarity">${Math.round(photo._similarity * 100)}% visual</span>` : "";
    return `<article class="photo-card">
      <div class="photo-image"><img src="${photo.thumbnail}" alt="Stylized placeholder for ${escapeHtml(photo.species)}">${color}${flag}<span class="photo-rating">${stars}</span>${similarity}</div>
      <div class="photo-details"><div class="photo-name" title="${escapeHtml(photo.filename)}">${escapeHtml(photo.filename)}</div><div class="photo-meta"><span>${escapeHtml(photo.camera_model.replace(/^.+? /, ""))}</span><span>${photo.date}</span></div><div class="photo-keywords">${escapeHtml(photo.species)} · ${escapeHtml(photo.location || "No location")}</div></div>
    </article>`;
  }

  function renderPhotoGrid(result) {
    $("photoGrid").style.setProperty("--thumb-size", `${state.view.thumbSize}px`);
    $("photoGrid").classList.toggle("hide-details", !state.view.showDetails);
    $("photoGrid").innerHTML = result.map(cardHtml).join("");
  }

  function renderMap(result) {
    const list = result.slice(0, 24).map((photo) => `<div class="map-list-item"><img src="${photo.thumbnail}" alt=""><div><strong>${escapeHtml(photo.filename)}</strong><span>${escapeHtml(photo.location || "EXIF coordinates")}</span></div></div>`).join("");
    const markers = result.slice(0, 28).map((photo, index) => {
      const left = 10 + ((photo.id * 37) % 80);
      const top = 10 + ((photo.id * 53) % 78);
      return `<button class="map-marker" style="left:${left}%;top:${top}%" title="${escapeHtml(photo.filename)}" type="button">${index + 1}</button>`;
    }).join("");
    $("mapView").innerHTML = `<div class="map-list"><div class="map-list-header">${result.length} matching locations</div>${list}</div><div class="mock-map"><div class="map-water"></div>${markers}</div>`;
  }

  function renderReview(result) {
    $("reviewView").innerHTML = result.map((photo) => `<article class="review-card"><img src="${photo.thumbnail}" alt="Stylized placeholder for ${escapeHtml(photo.species)}"><div class="review-main"><h3>${escapeHtml(photo.prediction)}</h3><p>${escapeHtml(photo.filename)} · ${escapeHtml(photo.camera_model)} · ${photo.rating} stars</p><div class="confidence-bar" title="${Math.round(photo.confidence * 100)}% confidence"><span style="width:${photo.confidence * 100}%"></span></div></div><div class="review-status">${Math.round(photo.confidence * 100)}% · Pending</div></article>`).join("");
  }

  function renderDuplicates(result) {
    const matchIds = new Set(result.map((photo) => photo.id));
    const groups = new Map();
    photos.filter((photo) => photo.duplicate_group).forEach((photo) => {
      if (!groups.has(photo.duplicate_group)) groups.set(photo.duplicate_group, []);
      groups.get(photo.duplicate_group).push(photo);
    });
    const visibleGroups = Array.from(groups.entries()).filter(([, members]) => members.some((photo) => matchIds.has(photo.id)));
    $("duplicatesView").innerHTML = visibleGroups.map(([group, members]) => {
      const matches = members.filter((photo) => matchIds.has(photo.id)).length;
      return `<section class="duplicate-group"><div class="duplicate-group-header"><strong>${group}</strong><span>${matches} matched · showing all ${members.length} members</span></div><div class="duplicate-members">${members.map((photo) => `<div class="duplicate-member ${matchIds.has(photo.id) ? "match" : ""}">${matchIds.has(photo.id) ? `<span class="match-badge">Matches filter</span>` : ""}<img src="${photo.thumbnail}" alt=""><span>${escapeHtml(photo.filename)}</span></div>`).join("")}</div></section>`;
    }).join("");
  }

  function labelColor(color) {
    return { red: "#ef635b", yellow: "#f6c945", green: "#6fcf75", blue: "#4aa9e9", purple: "#a782ff" }[color] || "transparent";
  }

  function renderBadges(result, hasVisualError) {
    const badges = [];
    if (pageState().muted) {
      const wouldMatch = applyUserFilters(pageState()).length;
      badges.push(`<span class="content-badge badge-paused">Filters paused — showing everything in scope · ${wouldMatch} would match · press \\ to resume</span>`);
    }
    if (pageState().visual?.status === "partial") badges.push(`<span class="content-badge">Visual index: ${photos.filter((p) => p.indexed).length} of ${photos.length} photos</span>`);
    if (hasVisualError) badges.push(`<span class="content-badge" style="color:var(--danger)">${pageState().visual.status === "unsupported" ? "Active model does not support text search" : "No visual index available"} · metadata filters shown only</span>`);
    if (state.context === "duplicates" && result.length) badges.push(`<span class="content-badge">Complete groups remain visible</span>`);
    $("contentBadges").innerHTML = badges.join("");
  }

  function updateViewControls() {
    $("sortSelect").value = state.view.sort;
    $("thumbSize").value = state.view.thumbSize;
    $("showDetails").checked = state.view.showDetails;
  }

  function hasUserFilters() {
    return Boolean(pageState().visual || pageState().root.children.length);
  }

  const PRESETS = [
    { id: "clear", title: "Clean slate", description: "No user filters; only the current page scope." },
    { id: "filename", title: "Owl filenames", description: "Filename contains owl and does not contain juvenile." },
    { id: "visual", title: "Visual + refinements", description: "Bird flying at dusk, picked, rated four stars or better." },
    { id: "advanced", title: "Advanced logic", description: "(Picked OR 5 stars) AND Sony camera." },
    { id: "facets", title: "Recent picks by facet", description: "Last 12 months, red or yellow label, ISO between 400 and 3200." },
    { id: "crowded", title: "Crowded filter bar", description: "Eight rules for testing chip overflow and editability." },
    { id: "clip_error", title: "Unavailable visual search", description: "Shows an unsupported-model clause without silently returning zero." },
  ];

  function renderPresets() {
    const saved = state.savedCollections.map((collection, index) => ({ id: `saved:${index}`, title: `Collection · ${collection.name}`, description: collection.summary }));
    $("presetList").innerHTML = PRESETS.concat(saved).map((preset) => `<button class="preset-button" data-preset="${preset.id}" type="button"><strong>${escapeHtml(preset.title)}</strong><span>${escapeHtml(preset.description)}</span></button>`).join("");
  }

  function applyPreset(id) {
    mutate(() => {
      const fresh = emptyPageState();
      if (id.startsWith("saved:")) {
        const saved = state.savedCollections[Number(id.split(":")[1])];
        const target = saved.includeScope && saved.context && CONTEXTS[saved.context] ? saved.context : state.context;
        state.pages[target] = clone(saved.page);
        if (target !== state.context) state.context = target;
        return;
      }
      if (id === "filename") {
        fresh.root.children = [makeRule("filename", "contains", "owl"), makeRule("filename", "not_contains", "juvenile")];
      } else if (id === "visual") {
        fresh.visual = { prompt: "bird flying at dusk", strength: "balanced", status: "partial" };
        fresh.root.children = [makeRule("rating", "gte", 4), makeRule("flag", "eq", "flagged")];
        state.view.sort = "relevance";
      } else if (id === "advanced") {
        fresh.advanced = true;
        fresh.root.children = [
          { kind: "group", match: "any", children: [makeRule("flag", "eq", "flagged"), makeRule("rating", "eq", 5)] },
          makeRule("camera_model", "contains", "Sony"),
        ];
      } else if (id === "facets") {
        fresh.root.children = [
          makeRule("date", "in_last", { n: 12, unit: "months" }),
          makeRule("color", "in", ["red", "yellow"]),
          makeRule("iso", "between", { from: 400, to: 3200 }),
        ];
      } else if (id === "crowded") {
        fresh.root.children = [
          makeRule("rating", "gte", 3), makeRule("flag", "neq", "rejected"), makeRule("color", "neq", "purple"),
          makeRule("iso", "lte", 3200), makeRule("camera_model", "contains", "Sony"), makeRule("focal_length", "gte", 400),
          makeRule("date", "gte", "2024-06-01"), makeRule("keyword", "not_contains", "juvenile"),
        ];
      } else if (id === "clip_error") {
        fresh.visual = { prompt: "owl in moonlight", strength: "balanced", status: "unsupported" };
        fresh.root.children = [makeRule("rating", "gte", 3)];
      }
      state.pages[state.context] = fresh;
    }, { clip: id === "visual" || id === "clip_error", message: id === "visual" ? "Encoding visual description…" : undefined });
  }

  function openFilterPopover(open) {
    const shouldOpen = open == null ? $("filterPopover").hidden : Boolean(open);
    $("filterPopover").hidden = !shouldOpen;
    $("filterButton").setAttribute("aria-expanded", String(shouldOpen));
    $("filterButton").classList.toggle("open", shouldOpen);
    if (shouldOpen) renderRules();
  }

  function closeMenus(except) {
    if (except !== "view") $("viewPopover").hidden = true;
    if (except !== "actions") $("actionsMenu").hidden = true;
    if (except !== "field") $("fieldPicker").hidden = true;
    if (except !== "search") hideSuggestions();
  }

  function renderFieldPicker(query) {
    const needle = String(query || "").trim().toLocaleLowerCase();
    const entries = Object.entries(FIELD_DEFS).filter(([key, def]) => {
      if (key === "all_text") return false;
      if (def.category.startsWith("This page") && state.context !== "review") return false;
      return !needle || `${def.label} ${def.category}`.toLocaleLowerCase().includes(needle);
    });
    const categories = new Map();
    entries.forEach(([key, def]) => {
      if (!categories.has(def.category)) categories.set(def.category, []);
      categories.get(def.category).push([key, def]);
    });
    $("fieldOptions").innerHTML = Array.from(categories.entries()).map(([category, fields]) => `<div class="field-category">${escapeHtml(category)}</div>${fields.map(([key, def]) => `<button class="field-option" data-add-field="${key}" type="button"><span>${escapeHtml(def.label)}</span><span>${escapeHtml(def.type)}</span></button>`).join("")}`).join("") || `<div style="padding:15px;color:var(--text-dim);text-align:center;font-size:10px;">No fields found</div>`;
  }

  function showValueSuggest(input) {
    const wrap = input.closest(".value-wrap");
    if (!wrap) return;
    const drop = wrap.querySelector(".value-suggest");
    const node = getRuleAtPath(input.dataset.path);
    if (!node || node.kind === "group") { drop.hidden = true; return; }
    const needle = String(input.value || "").trim().toLocaleLowerCase();
    const counts = new Map();
    photos.filter((photo) => matchesExceptRule(photo, node)).forEach((photo) => {
      const raw = fieldValue(photo, node.field);
      (Array.isArray(raw) ? raw : [raw]).filter((value) => !isMissing(value)).forEach((value) => counts.set(value, (counts.get(value) || 0) + 1));
    });
    const options = Array.from(counts.entries())
      .filter(([value]) => !needle || String(value).toLocaleLowerCase().includes(needle))
      .sort((a, b) => b[1] - a[1] || String(a[0]).localeCompare(String(b[0])))
      .slice(0, 8);
    if (!options.length) { drop.hidden = true; return; }
    drop.innerHTML = `<div class="value-suggest-hint">In your photos · counts respect other filters</div>${options.map(([value, count]) =>
      `<button class="value-option" data-suggest-value="${escapeHtml(value)}" data-path="${input.dataset.path}" type="button"><span>${escapeHtml(value)}</span><em>${count}</em></button>`
    ).join("")}`;
    drop.hidden = false;
  }

  function hideValueSuggests() {
    document.querySelectorAll(".value-suggest").forEach((drop) => { drop.hidden = true; });
  }

  function showSuggestions() {
    const query = $("quickSearch").value.trim();
    if (!query) { hideSuggestions(); return; }
    const suggestions = [
      { type: "all_text", icon: "⌕", title: `Search all text for “${query}”`, detail: "Filename, folder, keywords, camera, lens, location", key: "Enter" },
      { type: "filename", icon: "Aa", title: `Filename contains “${query}”`, detail: "Only the file name", key: "" },
      { type: "keyword", icon: "#", title: `Keyword contains “${query}”`, detail: "Assigned keywords and species tags", key: "" },
      { type: "visual", icon: "✦", title: `Visually similar to “${query}”`, detail: "Uses the active CLIP-compatible model", key: "" },
    ];
    $("searchSuggestions").innerHTML = suggestions.map((item, index) => `<button class="suggestion ${index === searchIndex ? "active" : ""}" data-search-type="${item.type}" type="button" role="option" aria-selected="${index === searchIndex}"><span class="suggestion-icon">${item.icon}</span><span><strong>${escapeHtml(item.title)}</strong><small>${escapeHtml(item.detail)}</small></span><em>${item.key}</em></button>`).join("");
    $("searchSuggestions").hidden = false;
    $("quickSearch").setAttribute("aria-expanded", "true");
  }

  function hideSuggestions() {
    $("searchSuggestions").hidden = true;
    $("quickSearch").setAttribute("aria-expanded", "false");
    searchIndex = -1;
  }

  function applySearch(type, query) {
    const value = String(query == null ? $("quickSearch").value : query).trim();
    if (!value) {
      const hasQuickSearch = allLeaves(pageState().root).some((node) => node.source === "quick_search") || pageState().visual?.source === "quick_search";
      if (hasQuickSearch) mutate(() => {
        removeQuickSearchRule();
        if (pageState().visual?.source === "quick_search") pageState().visual = null;
      });
      hideSuggestions();
      return;
    }
    if (type === "visual") {
      mutate(() => {
        removeQuickSearchRule();
        pageState().visual = { prompt: value, strength: "balanced", status: "partial", source: "quick_search" };
        state.view.sort = "relevance";
      }, { clip: true, message: "Encoding visual description…" });
    } else {
      mutate(() => {
        removeQuickSearchRule();
        pageState().visual = null;
        const rule = makeRule(type || "all_text", "contains", value);
        rule.source = "quick_search";
        pageState().root.children.unshift(rule);
      });
    }
    $("quickSearch").value = value;
    hideSuggestions();
  }

  function removeQuickSearchRule() {
    pageState().root.children = pageState().root.children.filter((node) => node.source !== "quick_search");
  }

  function syncQuickSearchInput() {
    if (document.activeElement === $("quickSearch")) return;
    const rule = allLeaves(pageState().root).find((node) => node.source === "quick_search");
    const visual = pageState().visual?.source === "quick_search" ? pageState().visual : null;
    $("quickSearch").value = visual?.prompt || rule?.value || "";
  }

  function clearFilters(withUndo) {
    mutate(() => { state.pages[state.context] = emptyPageState(); });
    if (withUndo) showUndoToast("Filters cleared");
  }

  function toggleMute() {
    if (!hasUserFilters() && !pageState().muted) { showToast("No filters to pause"); return; }
    mutate(() => { pageState().muted = !pageState().muted; });
    showToast(pageState().muted ? "Filters paused — press \\ to resume" : "Filters resumed");
  }

  function openModal(id) {
    $("modalBackdrop").hidden = false;
    $(id).hidden = false;
  }

  function closeModals() {
    $("modalBackdrop").hidden = true;
    $("saveModal").hidden = true;
    $("handoffModal").hidden = true;
  }

  function openSaveModal() {
    closeMenus();
    $("collectionName").value = `Filtered ${CONTEXTS[state.context].title} photos`;
    // Preview the count that will actually be saved: confirmSave stores the
    // Collection with muted:false, so when the user is paused we must show the
    // filtered count (not getFilteredPhotos()'s "everything in scope").
    const count = pageState().muted ? applyUserFilters(pageState()).length : getFilteredPhotos().length;
    $("savePreview").innerHTML = `<strong>${count} matching photos</strong><br>${escapeHtml(expressionSummary())}`;
    openModal("saveModal");
    setTimeout(() => $("collectionName").select(), 0);
  }

  function openHandoffModal() {
    closeMenus();
    $("handoffOptions").innerHTML = Object.entries(CONTEXTS).filter(([key]) => key !== state.context).map(([key, value]) => `<button class="handoff-option" data-handoff="${key}" type="button"><strong>${escapeHtml(value.title)}</strong><span>Adds: ${escapeHtml(value.scope)}</span></button>`).join("");
    openModal("handoffModal");
  }

  function switchContext(context, transfer) {
    if (!CONTEXTS[context]) return;
    if (transfer) state.pages[context] = clone(pageState());
    state.context = context;
    persist();
    closeModals();
    render();
    showToast(transfer ? `Filters opened in ${CONTEXTS[context].title}; its page scope was added.` : `${CONTEXTS[context].title} restored its own filters.`);
  }

  function resetPrototype() {
    if (!window.confirm("Reset every page, saved prototype Collection, checklist item, and display preference?")) return;
    state = defaultState();
    persist();
    closeModals();
    openFilterPopover(false);
    renderChecklist();
    render();
    showToast("Prototype reset");
  }

  function renderChecklist() {
    document.querySelectorAll("#evaluationChecklist input").forEach((input) => { input.checked = Boolean(state.checklist[input.dataset.check]); });
  }

  function installEvents() {
    document.querySelectorAll(".context-tab").forEach((button) => button.addEventListener("click", () => switchContext(button.dataset.context, false)));
    $("filterButton").addEventListener("click", () => { closeMenus(); openFilterPopover(); });
    $("closeFilter").addEventListener("click", () => openFilterPopover(false));
    $("doneFilter").addEventListener("click", () => openFilterPopover(false));
    $("overflowChip").addEventListener("click", () => openFilterPopover(true));
    $("clearAll").addEventListener("click", () => clearFilters(true));
    $("emptyClear").addEventListener("click", () => clearFilters(true));
    $("clearRules").addEventListener("click", () => clearFilters(true));
    $("resetPrototype").addEventListener("click", resetPrototype);

    $("quickSearch").addEventListener("input", () => { searchIndex = -1; showSuggestions(); });
    $("quickSearch").addEventListener("focus", showSuggestions);
    $("quickSearch").addEventListener("keydown", (event) => {
      const suggestions = Array.from($("searchSuggestions").querySelectorAll(".suggestion"));
      if (event.key === "ArrowDown") { event.preventDefault(); searchIndex = Math.min(suggestions.length - 1, searchIndex + 1); showSuggestions(); }
      else if (event.key === "ArrowUp") { event.preventDefault(); searchIndex = Math.max(0, searchIndex - 1); showSuggestions(); }
      else if (event.key === "Enter") { event.preventDefault(); applySearch(suggestions[searchIndex]?.dataset.searchType || "all_text"); }
      else if (event.key === "Escape") hideSuggestions();
    });
    $("searchSuggestions").addEventListener("click", (event) => {
      const button = event.target.closest("[data-search-type]");
      if (button) applySearch(button.dataset.searchType);
    });

    $("quickStars").addEventListener("click", (event) => {
      const button = event.target.closest("[data-rating]");
      if (button) setQuickRule("rating", $("quickRatingOp").value, Number(button.dataset.rating));
    });
    $("quickRatingOp").addEventListener("change", (event) => {
      const rating = findRootRule("rating");
      if (rating) mutate(() => { rating.op = event.target.value; });
      else renderQuickFilters();
    });
    $("quickFlags").addEventListener("click", (event) => {
      const button = event.target.closest("[data-flag]");
      if (button) toggleQuickEnum("flag", button.dataset.flag);
    });
    $("quickColors").addEventListener("click", (event) => {
      const button = event.target.closest("[data-color]");
      if (button) toggleQuickEnum("color", button.dataset.color);
    });
    $("muteFilters").addEventListener("click", toggleMute);

    $("advancedToggle").addEventListener("change", (event) => {
      // The toggle controls whether "＋ Group" is offered; it does NOT rewrite
      // the tree. Flattening a grouped expression on toggle-off silently changed
      // (A OR B) AND C into A AND B AND C and turned Match-none groups into
      // positive filters, so the result set moved when the user only wanted to
      // hide advanced controls.
      mutate(() => { pageState().advanced = event.target.checked; });
    });
    $("addGroupButton").addEventListener("click", () => mutate(() => pageState().root.children.push({ kind: "group", match: "any", children: [makeRule("flag", "eq", "flagged"), makeRule("rating", "eq", 5)] })));
    $("addFilterButton").addEventListener("click", () => {
      const next = $("fieldPicker").hidden;
      closeMenus(next ? "field" : undefined);
      $("fieldPicker").hidden = !next;
      $("addFilterButton").setAttribute("aria-expanded", String(next));
      if (next) { renderFieldPicker(""); setTimeout(() => $("fieldSearch").focus(), 0); }
    });
    $("fieldSearch").addEventListener("input", (event) => renderFieldPicker(event.target.value));
    $("fieldOptions").addEventListener("click", (event) => {
      const button = event.target.closest("[data-add-field]");
      if (!button) return;
      mutate(() => pageState().root.children.push(makeRule(button.dataset.addField)));
      $("fieldPicker").hidden = true;
    });

    $("ruleTree").addEventListener("change", handleRuleEvent);
    $("ruleTree").addEventListener("input", (event) => {
      if (["value-input", "value-from", "value-to", "rel-n"].includes(event.target.dataset.action)) {
        clearTimeout(clipTimer);
        clipTimer = setTimeout(() => handleRuleEvent(event), 180);
      }
      if (event.target.dataset.suggest) showValueSuggest(event.target);
    });
    $("ruleTree").addEventListener("focusin", (event) => {
      if (event.target.dataset && event.target.dataset.suggest) showValueSuggest(event.target);
    });
    $("ruleTree").addEventListener("click", (event) => {
      const suggestion = event.target.closest("[data-suggest-value]");
      if (suggestion) {
        mutate(() => { getRuleAtPath(suggestion.dataset.path).value = suggestion.dataset.suggestValue; });
        hideValueSuggests();
        return;
      }
      const target = event.target.closest("[data-action]");
      if (!target) return;
      const action = target.dataset.action;
      const path = target.dataset.path;
      if (action === "multi-toggle") {
        mutate(() => {
          const node = getRuleAtPath(path);
          let values = Array.isArray(node.value) ? node.value.slice() : [node.value];
          if (values.includes(target.dataset.value)) values = values.filter((entry) => entry !== target.dataset.value);
          else values.push(target.dataset.value);
          node.value = values;
        });
        return;
      }
      if (["field", "op", "value", "boolean", "value-input", "match", "case", "value-from", "value-to", "rel-n", "rel-unit"].includes(action)) return;
      if (action === "remove") {
        mutate(() => { const ref = getParentAtPath(path); ref.parent.children.splice(ref.index, 1); });
        showUndoToast("Rule removed");
      } else if (action === "add-child") mutate(() => getRuleAtPath(path).children.push(makeRule("keyword")));
      else if (action === "add-subgroup") mutate(() => getRuleAtPath(path).children.push({ kind: "group", match: "all", children: [makeRule("rating", "gte", 3)] }));
    });

    $("chipList").addEventListener("click", (event) => {
      const remove = event.target.closest("[data-remove-chip]");
      const chip = event.target.closest("[data-chip-index]");
      if (!chip) return;
      const index = Number(chip.dataset.chipIndex);
      const entries = [];
      if (pageState().visual) entries.push({ type: "visual" });
      allLeaves(pageState().root).forEach((rule) => entries.push({ type: "rule", rule }));
      if (remove) {
        event.stopPropagation();
        mutate(() => {
          const entry = entries[index];
          if (entry.type === "visual") pageState().visual = null;
          else removeRuleByReference(pageState().root, entry.rule);
        });
        showUndoToast("Filter removed");
      } else openFilterPopover(true);
    });

    $("viewButton").addEventListener("click", () => {
      const next = $("viewPopover").hidden;
      closeMenus(next ? "view" : undefined);
      $("viewPopover").hidden = !next;
      $("viewButton").setAttribute("aria-expanded", String(next));
    });
    $("sortSelect").addEventListener("change", (event) => { state.view.sort = event.target.value; persist(); render(); });
    $("thumbSize").addEventListener("input", (event) => { state.view.thumbSize = Number(event.target.value); persist(); renderPhotoGrid(getFilteredPhotos()); });
    $("showDetails").addEventListener("change", (event) => { state.view.showDetails = event.target.checked; persist(); renderPhotoGrid(getFilteredPhotos()); });

    $("moreActions").addEventListener("click", () => {
      const next = $("actionsMenu").hidden;
      closeMenus(next ? "actions" : undefined);
      $("actionsMenu").hidden = !next;
      $("moreActions").setAttribute("aria-expanded", String(next));
    });
    $("saveCollection").addEventListener("click", openSaveModal);
    $("openResults").addEventListener("click", openHandoffModal);
    $("copyExpression").addEventListener("click", async () => {
      const text = `${CONTEXTS[state.context].scope}: ${expressionSummary()}`;
      try { await navigator.clipboard.writeText(text); showToast("Filter summary copied"); }
      catch (_error) { showToast(text); }
      closeMenus();
    });
    $("confirmSave").addEventListener("click", () => {
      const name = $("collectionName").value.trim() || "Untitled Collection";
      const savedPage = clone(pageState());
      savedPage.muted = false;
      state.savedCollections.push({ name, page: savedPage, summary: expressionSummary(), includeScope: $("includeScope").checked, context: state.context });
      state.checklist.save = true;
      persist();
      closeModals();
      renderChecklist();
      renderPresets();
      showToast(`Saved “${name}” in the prototype`);
    });
    $("handoffOptions").addEventListener("click", (event) => {
      const button = event.target.closest("[data-handoff]");
      if (button) { state.checklist.contexts = true; switchContext(button.dataset.handoff, true); renderChecklist(); }
    });
    document.querySelectorAll(".modal-close").forEach((button) => button.addEventListener("click", closeModals));
    $("modalBackdrop").addEventListener("click", closeModals);

    $("presetList").addEventListener("click", (event) => {
      const button = event.target.closest("[data-preset]");
      if (button) applyPreset(button.dataset.preset);
    });
    $("evaluationChecklist").addEventListener("change", (event) => {
      if (!event.target.dataset.check) return;
      state.checklist[event.target.dataset.check] = event.target.checked;
      persist();
    });
    $("clearChecklist").addEventListener("click", () => { state.checklist = {}; persist(); renderChecklist(); });
    $("themeToggle").addEventListener("click", () => { state.theme = state.theme === "vireo-dark" ? "vireo-light" : "vireo-dark"; persist(); render(); });

    document.addEventListener("keydown", (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLocaleLowerCase() === "f") { event.preventDefault(); $("quickSearch").focus(); }
      const typing = ["INPUT", "SELECT", "TEXTAREA"].includes(event.target.tagName) || event.target.isContentEditable;
      if (event.key === "\\" && !typing && !event.metaKey && !event.ctrlKey) { event.preventDefault(); toggleMute(); }
      if (event.key === "Escape") { closeMenus(); closeModals(); hideValueSuggests(); if (!$("filterPopover").hidden) openFilterPopover(false); }
    });
    document.addEventListener("click", (event) => {
      if (!event.target.closest(".search-wrap")) hideSuggestions();
      if (!event.target.closest(".value-wrap")) hideValueSuggests();
      if (!event.target.closest(".view-wrap")) $("viewPopover").hidden = true;
      if (!event.target.closest(".more-button") && !event.target.closest(".actions-menu")) $("actionsMenu").hidden = true;
      if (!event.target.closest(".field-picker-wrap")) $("fieldPicker").hidden = true;
    });
    window.addEventListener("resize", updateChipOverflow);
  }

  function handleRuleEvent(event) {
    const action = event.target.dataset.action;
    const path = event.target.dataset.path;
    if (!action || path == null) return;
    mutate(() => {
      const node = getRuleAtPath(path);
      if (action === "field") {
        node.field = event.target.value;
        const def = FIELD_DEFS[node.field];
        node.op = defaultOperator(def);
        node.value = defaultValue(node.field, node.op);
      } else if (action === "op") {
        node.op = event.target.value;
        node.value = coerceValue(node.field, node.op, node.value);
      } else if (action === "value") node.value = event.target.value;
      else if (action === "boolean") node.value = event.target.value === "true";
      else if (action === "value-input") {
        const type = FIELD_DEFS[node.field].type;
        node.value = ["number", "rating"].includes(type) ? Number(event.target.value) : event.target.value;
      } else if (action === "value-from" || action === "value-to") {
        const def = FIELD_DEFS[node.field];
        const parsed = def.type === "date" ? event.target.value : Number(event.target.value);
        node.value = { ...(node.value || {}), [action === "value-from" ? "from" : "to"]: parsed };
      } else if (action === "rel-n") node.value = { ...(node.value || {}), n: Math.max(1, Number(event.target.value) || 1) };
      else if (action === "rel-unit") node.value = { ...(node.value || {}), unit: event.target.value };
      else if (action === "case") node.caseSensitive = event.target.checked;
      else if (action === "match") node.match = event.target.value;
    }, { noSnapshot: ["value-input", "value-from", "value-to", "rel-n"].includes(action) });
  }

  function removeRuleByReference(node, rule) {
    if (node.kind !== "group") return false;
    const index = node.children.indexOf(rule);
    if (index >= 0) { node.children.splice(index, 1); return true; }
    return node.children.some((child) => child.kind === "group" && removeRuleByReference(child, rule));
  }

  installEvents();
  renderChecklist();
  renderFieldPicker("");
  render();
})();
