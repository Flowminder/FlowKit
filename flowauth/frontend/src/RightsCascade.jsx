/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import React from "react";
import { MultiCascader } from "rsuite";
import "rsuite/dist/styles/rsuite-default.css";

function scopeStringListToGraph(array) {
  const nested = {};
  array.forEach(scope => {
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

function jsonify(tree, labels) {
  const list = Object.keys(tree).map(k => {
    const ll = labels.concat([k]);
    const v = tree[k];
    if (Object.keys(v).length === 0) {
      return { label: k, value: ll.join(":") };
    } else {
      return { label: k, value: ll.join(":"), children: jsonify(v, ll) };
    }
  });
  return list;
}

class RightsCascade extends React.Component {
  state = { data: [] };
  async componentDidMount() {
    const { srcData } = this.props;
    this.setState({ data: Object.values(srcData) });
  }
  render() {
    const { srcData } = this.props;
    if (srcData) {
      const data = scopeStringListToGraph(Object.keys(srcData));
      const merged = jsonify(data, []);
      return (
        <MultiCascader
          block
          data={merged}
          value={Object.keys(srcData)}
          preventOverflow
          menuWidth={400}
        />
      );
    }
  }
}

export default RightsCascade;
