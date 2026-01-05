#!/usr/bin/env bash

# Total width of the whole line
WIDTH=100   # adjust if you want longer/shorter lines

print_heading() {
    local title="$1"
    local prefix="## —— "
    local suffix=" "
    local dash="-"

    # length of the visible title block
    local block_len=$(( ${#prefix} + ${#title} + ${#suffix} ))

    # number of dashes needed
    local dash_count=$(( WIDTH - block_len ))

    # build dash line
    local dashes
    dashes=$(printf "%*s" "$dash_count" "" | tr ' ' "$dash")

    # print the final heading
    printf "%s%s%s%s\n" "$prefix" "$title" "$suffix" "$dashes"
}

print_heading "$1"
