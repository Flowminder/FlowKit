/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import { EmojiObjects } from "@material-ui/icons";

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

/** Takes an array of scopes, most of which are triplets in the form admin_level:top_level_query:dependent_query
 * and converts them into a graph keyed on admin_level -> top_level_query -> dependent_query
 */
export function scopesGraph(scopes_obj) {
  var scopes_array;
  if (!Array.isArray(scopes_obj)) {
    scopes_array = Object.keys(scopes_obj);
  } else {
    scopes_array = scopes_obj;
  }
  const triplet_scopes = scopes_array.filter((row) => row.includes(":")); // It's this or a regex
  const split_scopes = triplet_scopes.map((row) => row.split(":"));
  const scopes_tree = {};
  for (const row of split_scopes) {
    var [admin_level, tl_query, inner_query] = row;

    if (!Object.keys(scopes_tree).includes(admin_level)) {
      scopes_tree[admin_level] = {};
    }
    if (!Object.keys(scopes_tree[admin_level]).includes(tl_query)) {
      scopes_tree[admin_level][tl_query] = {};
    }
    scopes_tree[admin_level][tl_query][inner_query] = true;
  }
  const single_scopes = scopes_array
    .filter((row) => !row.includes(":"))
    .map((scope) => ({ [scope]: true }))
    .reduce((p_x, x) => ({ ...p_x, ...x }), []);
  const all_scopes = { ...scopes_tree, ...single_scopes };
  console.log(all_scopes);
  return all_scopes;
}

function jsonify_inner(tree, label, value, enabled) {
  const things = Object.keys(tree).map((branch, index) => {
    const this_value = value === "" ? branch : [label, branch].join(":");
    const this_label = label === "" ? branch : branch;
    if (typeof tree[branch] === "boolean") {
      if (tree[branch]) {
        enabled.push(this_value);
      }
      return {
        label: this_label,
        value: this_value,
        enabled: enabled.includes(this_value),
      };
    } else {
      return {
        label: this_label,
        value: this_value,
        children: jsonify_inner(tree[branch], this_label, this_value, enabled),
      };
    }
  });
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
export function jsonify(tree, enabled_keys) {
  const out = jsonify_inner(tree, "", "", enabled_keys);
  //I don't like that this has enabled_keys as a return argVx
  console.log("Test jsonify", out);
  console.log("Enabled keys:", enabled_keys);
  return out;
}

/**
 * Walks through two trees, returning the list of roots that lead to trees that match
 * @param {*} scopes_1
 * @param {*} scopes_2
 *
 * Notes: The algorithm is as follows.
 * - Start at the root of two trees (a set of nested objects {root:{branch_1:{...}, branch_2:{...}}})
 * - Save the key of the root
 * - Stop if you are on a leaf of a tree
 * - Stop if subtree_1[root] == subtree_2[root]
 * -
 *
 */
export function highest_common_roots(scopes_1, scopes_2) {
  console.debug("HCR for:", scopes_1, scopes_2);
  const out = [];
  hrc_inner(scopes_1, scopes_2, out);
  console.debug("HCR:", out);
  return out;
}

function hrc_inner(scopes_1, scopes_2, out) {
  // Type check here
  const s1_keys = Object.keys(scopes_1);
  const s2_keys = Object.keys(scopes_2);

  const key_intersection = s1_keys.filter((x) => s2_keys.includes(x));
  key_intersection.forEach((key) => {
    out.push(key);
    console.debug("HCR on", key, scopes_1[key], scopes_2[key]);
    if (
      Object.values(scopes_1[key]).every((x) => typeof x === "boolean") &&
      Object.values(scopes_2[key]).every((x) => typeof x === "boolean")
    ) {
      console.debug("Bottom of tree hit");
    }
    if (compare_trees(scopes_1[key], scopes_2[key])) {
      console.debug("Common key found:", key);
    } else {
      hrc_inner(scopes_1[key], scopes_2[key], out);
    }
  });
}

/**
 * Helper function that returns true if every member of array 1 is in array 2 and vice-versa
 * Only works on flat arrays - object comparison is always false in JS
 * @param {Array} arr1
 * @param {Array} arr2
 * @returns bool
 */
function do_arrays_match(arr1, arr2) {
  return (
    arr1.every((x) => arr2.includes(x)) && arr2.every((x) => arr1.includes(x))
  );
}

function compare_trees(t1, t2) {
  console.debug("comparing trees:", t1, t2);
  //Do t1 and t2 not have the same keys?
  if (!do_arrays_match(Object.keys(t1), Object.keys(t2))) {
    return false;
  }
  // Is this the bottom of t1 and t2?
  if (do_arrays_match(Object.values(t1), Object.values(t2))) {
    // If we are at the bottom of t1 and t2,
    return true;
  }
  if (
    Object.values(t1).every((x) => typeof x === "object") &&
    Object.values(t2).every((x) => typeof x === "object")
  ) {
    return Object.keys(t1).every((key) => compare_trees(t1[key], t2[key]));
  }
}

export function scopes_with_roles(roles) {
  //Rotates a list of roles-with-scopes to a list of
  //scopes-from-roles
  const scopes_obj = {};
  roles.forEach((role) => {
    role.scopes.forEach((scope) => {
      if (scopes_obj[scope] === undefined) {
        scopes_obj[scope] = [];
      }
      scopes_obj[scope].push(role.name);
    });
  });
  const scopes_out = [];
  for (let scope in scopes_obj) {
    scopes_out.push({
      name: scope,
      roles: scopes_obj[scope],
    });
  }

  return scopes_out;
}
