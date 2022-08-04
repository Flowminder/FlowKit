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

export function scopesGraphOld(array) {
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

  /** Takes an array of scopes, most of which are triplets in the form admin_level:top_level_query:dependent_query
  * and converts them into a graph keyed on admin_level -> top_level_query -> dependent_query
  */
export function scopesGraph(scopes_obj) {
  const scopes_array = Object.keys(scopes_obj)
  const triplet_scopes = scopes_array.filter(row => row.includes(":"))  // It's this or a regex
  const split_scopes = triplet_scopes.map(row => row.split(":"))
  const scopes_tree = {}
  for (const row of split_scopes){
    var [ admin_level, tl_query, inner_query ] = row

    if (!(Object.keys(scopes_tree).includes(admin_level))){
      scopes_tree[admin_level] = {}
    }
    if (!(Object.keys(scopes_tree[admin_level]).includes(tl_query))){
      scopes_tree[admin_level][tl_query] = {}
    }
    scopes_tree[admin_level][tl_query][inner_query] = true
  }
  const single_scopes=scopes_array.filter(row => !row.includes(":"))
    .map(scope => ({[scope]:true}))
    .reduce((p_x, x) => ({...p_x, ...x}))
  const all_scopes = ({...scopes_tree, ...single_scopes})
  console.log(all_scopes)
  return all_scopes
}


export function jsonify_inner(tree, label, value, enabled){
  const things = Object.keys(tree).map((branch, index) => {
    const this_label = label ==="" ? branch: [label, branch].join(".")
    const this_value = this_label
    // const this_value = value === "" ? index.toString(): [value, index.toString()].join("-")
    if (typeof(tree[branch]) === "boolean"){
      if (tree[branch]){
        enabled.push(this_value)
      }
      return {
        label: this_label,
        value: this_value,
        enabled: this_value in enabled
      }
    }
    else {
      return {
        label: this_label,
        value: this_value,
        children: jsonify_inner(tree[branch], this_label, this_value, enabled)
      }
    }
  })
  return things;
}

/**
 * Walks a tree of nested Objects and returns a tree in the form
 * object{label, value, children{[label, value, children....]}}
 * Will also populate enabledKeys with 
 * For use with RightsCascade
 * @param {*} tree 
 * @param {*} enabled_keys 
 * @returns 
 */
export function jsonify(tree, enabled_keys){
  const out = jsonify_inner(tree, "", "", enabled_keys)
//I don't like that this has enabled_keys as a return argVx
  console.log("Test jsonify", out)
  console.log("Enabled keys:", enabled_keys)
  return out
}


export function jsonifyOld(tree, labels, enabled, enabledKeys) {
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