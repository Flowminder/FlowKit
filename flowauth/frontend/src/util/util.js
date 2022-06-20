/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

function dropWhile(func) {
  let arr = this;
  while (arr.length > 0 && !func(arr[0])) {
    arr = arr.slice(1);
  }
  return arr;
}

Array.prototype.dropWhile = dropWhile;

function zip() {
  /* https://stackoverflow.com/a/10284006 */
  var args = [].slice.call(arguments);
  var longest = args.reduce(function (a, b) {
    return a.length > b.length ? a : b;
  }, []);

  return longest.map(function (_, i) {
    return args.map(function (array) {
      return array[i];
    });
  });
}

export function scopesGraph(array) {
  const nested = {};
  Object.keys(array).forEach((scope) => {
    let obj = nested;
    let last_split = [];
    let parents = [];
    scope.split("&").forEach((sub_scope) => {
      obj["parent"] = parents.join("&");
      let split = sub_scope.split(".");
      split = zip(last_split, split)
        .dropWhile((x) => x[0] !== x[1])
        .map((x) => x[1])
        .filter((x) => x !== undefined);
      split.forEach((k, ix) => {
        if (!(k in obj)) {
          obj[k] = {};
        }
        obj = obj[k];
        obj["parent"] = parents.join("&");
      });
      obj["full_path"] = sub_scope;
      last_split = split;
      parents.push(sub_scope);
    });
  });
  return nested;
}

export function jsonify(tree, labels, enabled, enabledKeys) {
  const parent = tree.parent;

  const list = Object.keys(tree)
    .filter((k) => k !== "parent" && k !== "full_path")
    .map((k) => {
      const ll = labels.concat([k]);
      const v = tree[k];
      const val = parent ? [parent, ll.join(".")].join("&") : ll.join(".");
      if (
        Object.keys(v).filter((k) => k !== "parent" && k !== "full_path")
          .length === 0
      ) {
        const value = [parent, v.full_path].join("&");
        return {
          label: k,
          value: value,
          parents: parent,
          enabled: enabled.includes(value),
        };
      } else {
        const children = jsonify(
          v,
          v.parent === parent ? ll : [],
          enabled,
          enabledKeys
        );
        const allEnabled = children.every((child) => child.enabled);
        if (!allEnabled) {
          children
            .filter((child) => child.enabled)
            .forEach((child) => enabledKeys.push(child.value));
        }
        return {
          label: k,
          value: val,
          children: children,
          parents: parent,
          enabled: allEnabled,
        };
      }
    });
  if (parent === "") {
    list
      .filter((obj) => obj.enabled)
      .forEach((obj) => enabledKeys.push(obj.value));
  }
  return list;
}


export function scopes_with_roles(roles){
  //Rotates a list of roles-with-scopes to a list of 
  //scopes-from-roles 
  const scopes_obj = {};
  roles.forEach((role) => {
    role.scopes.forEach((scope) => {
      if (scopes_obj[scope] == undefined){
        scopes_obj[scope] = []
      }
      scopes_obj[scope].push(role.name);
    })
  });
  const scopes_out = []
  for (let scope in scopes_obj){
    scopes_out.push({
      name: scope,
      roles : scopes_obj[scope]
    })
  }
  
  return scopes_out
}