# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mo_dots import wrap
from mo_logs import Log


class Line:
    '''
    Extend from this class to use apply_diff
    in other contexts so that other data can
    be moved around alongside the line.
    '''

    def __init__(self, linenum, is_new_line=False, filename=''):
        self.line = linenum
        self.is_new_line = is_new_line
        self.filename = filename

    def move_down(self):
        self.line = self.line + 1
        return self

    def move_up(self):
        self.line = self.line - 1
        return self

    def __str__(self):
        return "Line{line=" + str(self.line) + "}"


class SourceFile:

    def __init__(self, filename, lines):
        '''
        Expects a filename and a list of objects
        that can have an attribute added to them
        if it doesn't already exist.
        :param filename: name of the file
        :param lines: list of data objects extending `Line`
        '''
        self.filename = filename.lstrip('/')
        self.lines = self._format_line_objects(lines) if lines else []

    def _format_line_objects(self, lines):
        fmt_lines = []
        for lineind, line_obj in enumerate(lines):
            linenum = lineind + 1

            if not hasattr(line_obj, 'line') or not hasattr(line_obj, 'filename'):
                Log.error("Line objects in SourceFile must extend Line class.")

            line_obj.line = line_obj.line if line_obj.line else linenum
            fmt_lines.append(line_obj)
        return fmt_lines

    def reset_new_lines(self):
        for line_obj in self.lines:
            line_obj.is_new_line = False

    def set_filenames(self):
        for line_obj in self.lines:
            line_obj.filename = self.filename

    def get_new_lines(self):
        return [line_obj for line_obj in self.lines if line_obj.is_new_line]

    def add_one(self, new_line_obj):
        start = new_line_obj.line
        self.lines = self.lines[:start - 1] +\
                     [new_line_obj] +\
                     [line_obj.move_down() for line_obj in self.lines[start - 1:]]

    def remove_one(self, linenum_to_remove):
        self.lines = self.lines[:linenum_to_remove - 1] + \
                     [line_obj.move_up() for line_obj in self.lines[linenum_to_remove:]]


def apply_diff(file, diff):
    '''
    Using a list of line numbers (`file`), we change the line
    numbers to reflect a given diff and return them. diff must
    be a diff object returned from get_diff(cset, file). Added
    lines are of type Line.

    :param file: A SourceFile object
    :param diff: unified diff from get_diff
    :param filename: name of file that the diff is applied to
    :return: file, lines_inserted
    '''
    # Ignore merges, they have duplicate entries.
    if diff['merge']:
        return file
    if file.filename.lstrip('/') == 'dev/null':
        file.lines = []
        return file

    for f_proc in diff['diffs']:
        new_fname = f_proc['new'].name.lstrip('/')
        old_fname = f_proc['old'].name.lstrip('/')
        if new_fname != file.filename and old_fname != file.filename:
            continue
        if old_fname != new_fname:
            if new_fname == 'dev/null':
                file.lines = []
                return file
            # Change the file name so that new lines
            # are correctly created.
            file.filename = new_fname

        f_diff = f_proc['changes']
        for change in f_diff:
            if change.action == '+':
                file.add_one(
                    Line(change.line + 1, is_new_line=True, filename=file.filename)
                )
            elif change.action == '-':
                file.remove_one(change.line + 1)
        break
    return file


def apply_diff_backwards(file, diff):
    '''
    Reverses the diff and applies it using `apply_diff`.
    :param file: A SourceFile object.
    :param diff: a unified diff from get_diff to be reversed, then applied
    :return:
    '''
    new_diffs = []
    for f_proc in diff['diffs']:
        new_f_proc = wrap({
            'old': {'name': f_proc['new'].name},
            'new': {'name': f_proc['old'].name},
            'changes': []
        })

        new_changes = []
        f_diff =  f_proc['changes'].copy()
        for change in f_diff:
            if change.action == '+':
                change.action = '-'
                new_changes.append(change)
            elif change.action == '-':
                change.action = '+'
                new_changes.append(change)

        # Reverse it because final changes need to
        # be done first when applied.
        new_f_proc['changes'] = new_changes[::-1]
        new_diffs.append(new_f_proc)

    return apply_diff(file, {'diffs': new_diffs, 'merge': diff['merge']})
