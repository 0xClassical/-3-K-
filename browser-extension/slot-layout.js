(function exposeSlotLayout(root) {
  "use strict";

  const SLOT_IDS = ["A", "B", "C", "D"];

  function tileBounds(workArea) {
    const leftWidth = Math.floor(workArea.width / 2);
    const topHeight = Math.floor(workArea.height / 2);
    const rightWidth = workArea.width - leftWidth;
    const bottomHeight = workArea.height - topHeight;
    return {
      A: { left: workArea.left, top: workArea.top, width: leftWidth, height: topHeight },
      B: { left: workArea.left + leftWidth, top: workArea.top, width: rightWidth, height: topHeight },
      C: { left: workArea.left, top: workArea.top + topHeight, width: leftWidth, height: bottomHeight },
      D: { left: workArea.left + leftWidth, top: workArea.top + topHeight, width: rightWidth, height: bottomHeight },
    };
  }

  function hasFourDistinctWindows(slots) {
    const ids = SLOT_IDS.map((slotId) => slots?.[slotId]?.windowId);
    return ids.every(Number.isInteger) && new Set(ids).size === SLOT_IDS.length;
  }

  const api = { SLOT_IDS, tileBounds, hasFourDistinctWindows };
  root.MemeQuantSlotLayout = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(globalThis);
