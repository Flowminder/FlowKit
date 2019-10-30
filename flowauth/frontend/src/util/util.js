/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
export function scopesGraph(array) {
  const nested = {};
  Object.keys(array).forEach(scope => {
    let obj = nested;
    scope.split(":").forEach(k => {
      if (!(k in obj)) {
        obj[k] = {};
      }
      obj = obj[k];
    });
  });
  return nested;
}

export function jsonify(tree, labels, enabled, enabledKeys) {
  const list = Object.keys(tree).map(k => {
    const ll = labels.concat([k]);
    const v = tree[k];
    if (Object.keys(v).length === 0) {
      return {
        label: k,
        value: ll.join(":"),
        enabled: enabled.includes(ll.join(":"))
      };
    } else {
      const children = jsonify(v, ll, enabled, enabledKeys);
      const allEnabled = children.every(child => child.enabled);
      if (!allEnabled) {
        children
          .filter(child => child.enabled)
          .forEach(child => enabledKeys.push(child.value));
      }
      return {
        label: k,
        value: ll.join(":"),
        children: children,
        enabled: allEnabled
      };
    }
  });
  if (labels.length === 0) {
    list.filter(obj => obj.enabled).forEach(obj => enabledKeys.push(obj.value));
  }
  return list;
}
