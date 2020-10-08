def stats2tab(acc, separator="\t"):
    stats = [
        {
            "num_calls": d[1],
            "self_time": d[2],
            "total_time": d[3],
            "self_time_per_call": d[2] / d[1],
            "total_time_per_call": d[3] / d[1],
            "file": (f[0] if f[0] != "~" else "").replace("\\", "/"),
            "line": f[1],
            "method": f[2].lstrip("<").rstrip(">")
        }
        for f, d, in acc.stats.items()
    ]

    return list2tab(stats, separator=separator)


def list2tab(rows, separator="\t"):
    from mo_json import value2json

    columns = set()
    for r in rows:
        columns |= set(r.keys())
    keys = list(columns)

    output = []
    for r in rows:
        output.append(separator.join(value2json(r.get(k)) for k in keys))

    return separator.join(keys) + "\n" + "\n".join(output)
