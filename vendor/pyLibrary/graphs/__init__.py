# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple


class Graph(object):
    def __init__(self, node_type=None):
        self.nodes = set()
        self.edges = set()
        self.node_type = node_type

    def verticies(self):
        return self.nodes

    def add_edge(self, edge):
        self.nodes |= {edge.parent, edge.child}
        self.edges.add(edge)

    def remove_children(self, node):
        self.edges = [e for e in self.edges if e.parent != node]

    def get_children(self, node):
        # FIND THE REVISION
        return set(c for p, c in self.edges if p == node)

    def get_parents(self, node):
        return set(p for p, c in self.edges if c == node)

    def get_edges(self, node):
        return set((p, c) for p, c in self.edges if p == node or c == node)

    def get_family(self, node):
        """
        RETURN ALL ADJACENT NODES
        """
        return self.get_children(node) | self.get_parents(node)


Edge = namedtuple("Edge", ["parent", "child"])


class Tree(Graph):

    def __init__(self, node_type=None):
        self.parents = {}
        self.node_type = node_type

    @property
    def nodes(self):
        return set(c for c, p in self.parents.items())

    @property
    def edges(self):
        return set(Edge(p, c) for c, p in self.parents.items())

    def add_edge(self, edge):
        self.parents[edge.child] = edge.parent

    def get_children(self, node):
        return set(c for c, p in self.parents.items())

    def get_parents(self, node):
        parent = self.parents.get(node)
        if parent == None:
            return set()
        else:
            return {parent}

    def get_edges(self, node):
        return [(p, c) for c, p in self.parents.items() if p == node or c == node]

    def get_parent(self, node):
        return self.parents.get(node)

    def get_path_to_root(self, node):
        output = []
        while node is not None:
            output.append(node)
            node = self.parents.get(node)
        return output
