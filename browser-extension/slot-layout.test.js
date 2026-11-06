"use strict";

const assert = require("node:assert/strict");
const { tileBounds, hasFourDistinctWindows } = require("./slot-layout.js");

const even = tileBounds({ left: 0, top: 0, width: 1920, height: 1040 });
assert.deepEqual(even.A, { left: 0, top: 0, width: 960, height: 520 });
assert.deepEqual(even.D, { left: 960, top: 520, width: 960, height: 520 });

const odd = tileBounds({ left: -1919, top: 12, width: 1919, height: 1039 });
assert.equal(odd.A.width + odd.B.width, 1919);
assert.equal(odd.A.height + odd.C.height, 1039);
assert.equal(odd.B.left + odd.B.width, 0);
assert.equal(odd.C.top + odd.C.height, 12 + 1039);

assert.equal(hasFourDistinctWindows({
  A: { windowId: 1 }, B: { windowId: 2 }, C: { windowId: 3 }, D: { windowId: 4 },
}), true);
assert.equal(hasFourDistinctWindows({
  A: { windowId: 1 }, B: { windowId: 1 }, C: { windowId: 3 }, D: { windowId: 4 },
}), false);

console.log("slot-layout tests passed");
