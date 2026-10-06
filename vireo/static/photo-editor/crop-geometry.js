// Crop clamping and how rotation, flip, and straighten carry a crop.
// Classic page script; load boot.js after all definitions.

function isFullCrop(crop) {
  return !crop || (
    Math.abs(Number(crop.x || 0)) < 0.0005 &&
    Math.abs(Number(crop.y || 0)) < 0.0005 &&
    Math.abs(Number(crop.w || 1) - 1) < 0.0005 &&
    Math.abs(Number(crop.h || 1) - 1) < 0.0005
  );
}

function clampCrop(crop) {
  var min = 0.02;
  var x = Number(crop.x) || 0;
  var y = Number(crop.y) || 0;
  var w = Number(crop.w) || 1;
  var h = Number(crop.h) || 1;
  w = Math.max(min, Math.min(1, w));
  h = Math.max(min, Math.min(1, h));
  x = Math.max(0, Math.min(1 - w, x));
  y = Math.max(0, Math.min(1 - h, y));
  return { x: x, y: y, w: w, h: h };
}

function ensureCrop(recipe) {
  if (!recipe.crop || typeof recipe.crop !== 'object') {
    recipe.crop = { x: 0, y: 0, w: 1, h: 1 };
  }
  recipe.crop = clampCrop(recipe.crop);
  return recipe.crop;
}

function cropFromTransformedCorners(crop, transformPoint) {
  var c = clampCrop(crop);
  var corners = [
    { x: c.x, y: c.y },
    { x: c.x + c.w, y: c.y },
    { x: c.x, y: c.y + c.h },
    { x: c.x + c.w, y: c.y + c.h },
  ].map(transformPoint);
  var xs = corners.map(function(p) { return p.x; });
  var ys = corners.map(function(p) { return p.y; });
  var x1 = Math.min.apply(Math, xs);
  var y1 = Math.min.apply(Math, ys);
  var x2 = Math.max.apply(Math, xs);
  var y2 = Math.max.apply(Math, ys);
  return clampCrop({ x: x1, y: y1, w: x2 - x1, h: y2 - y1 });
}

function rotatePointInBox(point, clockwiseDegrees, aspect) {
  var angle = Number(clockwiseDegrees || 0) * Math.PI / 180;
  if (!angle || !Number.isFinite(aspect) || aspect <= 0) return point;
  var cos = Math.cos(angle);
  var sin = Math.sin(angle);
  var dx = (point.x - 0.5) * aspect;
  var dy = point.y - 0.5;
  return {
    x: ((dx * cos - dy * sin) / aspect) + 0.5,
    y: (dx * sin + dy * cos) + 0.5,
  };
}

function imageAspectForTransform(delta) {
  var img = document.getElementById('editorImg');
  var dims = editorNativeRecipeDimensions(editorState.recipe || {}, false);
  // A committed preview contains only the crop, but crop-coordinate
  // transforms are defined against the whole post-transform source.
  var useLoadedImage = !editorPreviewAppliesCrop() && img &&
    img.clientWidth && img.clientHeight && editorImageMatchesZoomRecipe(img);
  var width = useLoadedImage ? img.clientWidth : (dims && dims.width);
  var height = useLoadedImage ? img.clientHeight : (dims && dims.height);
  if (!width || !height) return null;
  var current = width / height;
  var normalizedDelta = ((Number(delta) || 0) % 360 + 360) % 360;
  return {
    current: current,
    next: normalizedDelta === 90 || normalizedDelta === 270 ? 1 / current : current,
  };
}

function transformCropWithStraighten(crop, transformPoint, straighten, aspects) {
  var angle = Number(straighten || 0);
  return cropFromTransformedCorners(crop, function(point) {
    var next = point;
    if (angle && aspects) next = rotatePointInBox(next, -angle, aspects.current);
    next = transformPoint(next);
    if (angle && aspects) next = rotatePointInBox(next, angle, aspects.next);
    return next;
  });
}

function transformCropForRotation(crop, delta, flip) {
  var normalizedDelta = ((Number(delta) || 0) % 360 + 360) % 360;
  var activeFlip = flip || {};
  var aspects = imageAspectForTransform(normalizedDelta);
  return transformCropWithStraighten(crop, function(point) {
    var x = point.x;
    var y = point.y;
    if (activeFlip.horizontal) x = 1 - x;
    if (activeFlip.vertical) y = 1 - y;
    if (normalizedDelta === 90) {
      var nextX = 1 - y;
      y = x;
      x = nextX;
    } else if (normalizedDelta === 180) {
      x = 1 - x;
      y = 1 - y;
    } else if (normalizedDelta === 270) {
      var nextY = 1 - x;
      x = y;
      y = nextY;
    }
    if (activeFlip.horizontal) x = 1 - x;
    if (activeFlip.vertical) y = 1 - y;
    return { x: x, y: y };
  }, editorState.recipe.straighten, aspects);
}

function transformCropForFlip(crop, axis) {
  var aspects = imageAspectForTransform(0);
  return transformCropWithStraighten(crop, function(point) {
    if (axis === 'horizontal') return { x: 1 - point.x, y: point.y };
    if (axis === 'vertical') return { x: point.x, y: 1 - point.y };
    return point;
  }, editorState.recipe.straighten, aspects);
}
