/* Grid request ownership is independent of rendering, paging and selection.
   Only a new dataset claims ownership; requests extending it observe the
   current owner. Old callbacks stay stale even if a later view looks identical. */
var sequence = 0;
var owner = {};

export function claimBrowseWindow() {
  owner = {};
  sequence++;
  return observeBrowseWindow();
}

export function observeBrowseWindow() {
  var observedOwner = owner;
  return function() { return owner === observedOwner; };
}

// The server's search lane needs a monotonic number to cancel older reads.
// UI callbacks use the opaque owner above, never compare sequence numbers.
export function browseWindowSequence() {
  return sequence;
}
