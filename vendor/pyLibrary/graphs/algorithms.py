# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from collections import deque

from mo_collections.queue import Queue

from mo_math import INTERSECT
from pyLibrary.graphs import Graph, Edge, Tree
from pyLibrary.graphs.paths import Step, Path
from mo_dots import Data


def dfs(graph, func, head, reverse=None):
    """
    DEPTH FIRST SEARCH

    IF func RETURNS FALSE, THEN PATH IS NO LONGER TAKEN

    IT'S EXPECTED func TAKES 3 ARGUMENTS
    node - THE CURRENT NODE IN THE
    path - PATH FROM head TO node
    graph - THE WHOLE GRAPH
    """
    todo = deque()
    todo.append(head)
    path = deque()
    done = set()
    while todo:
        node = todo.popleft()
        if node in done:
            path.pop()
            continue

        done.add(node)
        path.append(node)
        result = func(node, path, graph)
        if result:
            if reverse:
                children = graph.get_parents(node)
            else:
                children = graph.get_children(node)
            todo.extend(children)


def bfs(graph, func, head, reverse=None):
    """
    BREADTH FIRST SEARCH

    IF func RETURNS FALSE, THEN NO MORE PATHS DOWN THE BRANCH ARE TAKEN

    IT'S EXPECTED func TAKES 3 ARGUMENTS
    node - THE CURRENT NODE IN THE
    path - PATH FROM head TO node
    graph - THE WHOLE GRAPH
    todo - WHAT'S IN THE QUEUE TO BE DONE
    """

    todo = deque()  # LIST OF PATHS
    todo.append(Step(None, head))

    while True:
        path = todo.popleft()
        keep_going = func(path.node, Path(path), graph, todo)
        if keep_going:
            todo.extend(Step(path, c) for c in graph.get_children(path.node))


def dominator(graph, head):
    # WE WOULD NEED DOMINATORS IF WE DO NOT KNOW THE TOPOLOGICAL ORDERING
    # DOMINATORS ALLOW US TO USE A REFERENCE TEST RESULT: EVERYTHING BETWEEN
    # dominator(node) AND node CAN BE TREATED AS PARALLEL-APPLIED CHANGESETS
    #
    # INSTEAD OF DOMINATORS, WE COULD USE MANY PERF RESULTS, FROM EACH OF THE
    # PARENT BRANCHES, AND AS LONG AS THEY ALL ARE PART OF A LONG LINE OF
    # STATISTICALLY IDENTICAL PERF RESULTS, WE CAN ASSUME THEY ARE A DOMINATOR

    visited = set()
    dom = Data(output=None)

    def find_dominator(node, path, graph, todo):
        if dom.output:
            return False
        if not todo:
            dom.output = node
            return False
        if node in visited:
            common = INTERSECT(p[1::] for p in todo)  # DO NOT INCLUDE head
            if node in common:
                dom.output = node  #ALL REMAINING PATHS HAVE node IN COMMON TOO
            return False
        return True

    bfs(graph, find_dominator, head)

    return dom.output


LOOPS = "LOOPS"
ROOTS = "ROOTS"


def dominator_tree(graph):
    """
    RETURN DOMINATOR FOREST
    THERE ARE TWO TREES, "ROOTS" and "LOOPS"
    ROOTS HAVE NO PARENTS
    LOOPS ARE NODES THAT ARE A MEMBER OF A CYCLE THAT HAS NO EXTRNAL PARENT

    roots = dominator_tree(graph).get_children(ROOTS)
    """
    todo = Queue()
    done = set()
    dominator = Tree(None)

    def next_node():
        nodes = list(graph.nodes)

        while True:
            if todo:
                output = todo.pop()
            elif nodes:
                output = nodes.pop()
            else:
                return

            if output not in done:
                yield output

    for node in next_node():
        parents = graph.get_parents(node)
        if not parents:
            # node WITHOUT parents IS A ROOT
            done.add(node)
            dominator.add_edge(Edge(ROOTS, node))
            continue

        not_done = [p for p in parents if p not in done]
        if not_done:
            # THERE ARE MORE parents TO DO FIRST
            more_todo = [p for p in not_done if p not in todo]
            if not more_todo:
                # ALL PARENTS ARE PART OF A CYCLE, MAKE node A ROOT
                done.add(node)
                dominator.add_edge(Edge(LOOPS, node))
            else:
                # DO THE PARENTS BEFORE node
                todo.push(node)
                for p in more_todo:
                    todo.push(p)
            continue

        # WE CAN GET THE DOMINATORS FOR ALL parents
        if len(parents) == 1:
            # SHORTCUT
            dominator.add_edge(Edge(parents[0], node))
            done.add(node)
            continue

        paths_from_roots = [
            list(reversed(dominator.get_path_to_root(p)))
            for p in parents
        ]

        if any(p[0] == ROOTS for p in paths_from_roots):
            # THIS OBJECT CAN BE REACHED FROM A ROOT, IGNORE PATHS FROM LOOPS
            paths_from_roots = [p for p in paths_from_roots if p[0] == ROOTS]

        # FIND COMMON PATH FROM root
        num_paths = len(paths_from_roots)
        for i, x in enumerate(zip(*paths_from_roots)):
            if x.count(x[0]) != num_paths:
                dom = paths_from_roots[0][i-1]
                break
        else:
            dom = paths_from_roots[0][-1]

        # TODO: dom CAN BE None; IF node CAN BE REACHED FROM A LEGIT root *AND* FROM A CYCLE THEN dom is None; WHICH MAY NOT BE LEGIT
        dominator.add_edge(Edge(dom, node))
        done.add(node)

    return dominator
