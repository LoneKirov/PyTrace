if __name__ == "__main__":
    import argparse

    aparser = argparse.ArgumentParser(description='Detect sequential streams.')
    aparser.add_argument('--trace', required=True, dest='trace', help='path to json trace file')
    aparser.add_argument('--csv', required=False, dest='csv', help='path to csv output file')
    aparser.add_argument('--plot', required=False, dest='plot', help='path to plot output file')
    aparser.add_argument('--after', required=False, dest='after', type=int, help='consider commands after')
    aparser.add_argument('--before', required=False, dest='before', type=int, help='consider commands before')

    args = aparser.parse_args()

    from json_reader import JsonReader
    from sata import Parser
    from trace_statistics import commandsToStats, commandsToStatCSV, Time, LBA, Length, Stream
    from sequential_stream import Detector
    from itertools import tee, zip_longest, dropwhile, takewhile

    commands = Parser(JsonReader(args.trace))
    commands = Detector(commands, attrName='stream1', tDelta=1)
    commands = Detector(commands, attrName='stream10', tDelta=10)
    commands = Detector(commands, attrName='stream100', tDelta=100)
    commands = Detector(commands, attrName='stream1000', tDelta=1000)
    commands = Detector(commands, attrName='stream10000', tDelta=10000)
    commands = Detector(commands, attrName='stream100000', tDelta=100000)
    if args.after is not None:
        commands = dropwhile(lambda c: c.sTime() / 1000000 < args.after, commands)
    if args.before is not None:
        commands = takewhile(lambda c: c.sTime() / 1000000 <= args.before, commands)

    outputs = []

    statFields = [Time(True), LBA(), Length(), Stream('stream1'), Stream('stream10'), Stream('stream100'), Stream('stream1000'), Stream('stream10000'), Stream('stream100000')]

    if args.csv is not None:
        outputs.append(lambda cmds: commandsToStatCSV(args.csv, cmds, fields=statFields))

    import matplotlib.pyplot as plt
    if args.plot is not None:
        def plotOutputter(cmds):
            from functools import reduce
            from collections import defaultdict
            from numpy import fromiter
            def plot(stats, key, rows, cols, ndx):
                def accumulator(accum, s):
                    accum[s[key]].append(s['Start Time'])
                    return accum
                streams = reduce(accumulator, stats, defaultdict(list))
                del streams[-1]
                ax = plt.subplot(rows, cols, ndx)
                dur = key.replace('stream', '')
                ax.set_title(dur + ' (ms) Streams', fontsize=5)
                ax.set_ylabel('Stream ID', fontsize=5)
                ax.set_xlabel('Time (sec)', fontsize=5)
                for i, t in streams.items():
                    plt.plot(t, [i for _ in range(len(t))], '.')
                plt.setp(ax.get_xticklabels(), fontsize=4)
                plt.setp(ax.get_yticklabels(), fontsize=4)
                #del streams[-1]
                ax = plt.subplot(rows, cols, ndx + 1)
                ax.set_title(dur + ' (ms) Stream Lengths (commands)', fontsize=5)
                ax.set_ylabel('Count', fontsize=5)
                ax.set_xlabel('Number of Commands', fontsize=5)
                plt.hist([ len(t) for _, t in streams.items() ], bins=100)
                plt.setp(ax.get_xticklabels(), fontsize=4)
                plt.setp(ax.get_yticklabels(), fontsize=4)
                ax = plt.subplot(rows, cols, ndx + 2)
                ax.set_title(dur + ' (ms) Stream Durations', fontsize=5)
                ax.set_ylabel('Count', fontsize=5)
                ax.set_xlabel('Duration (sec)', fontsize=5)
                plt.hist([ t[len(t) - 1] - t[0] for _, t in streams.items() ], bins=100)
                plt.setp(ax.get_xticklabels(), fontsize=4)
                plt.setp(ax.get_yticklabels(), fontsize=4)
            stats = tee(commandsToStats(cmds, fields=statFields), 6)
            plot(stats[0], 'stream1', 6, 3, 1)
            plot(stats[1], 'stream10', 6, 3, 4)
            plot(stats[2], 'stream100', 6, 3, 7)
            plot(stats[3], 'stream1000', 6, 3, 10)
            plot(stats[4], 'stream10000', 6, 3, 13)
            plot(stats[5], 'stream100000', 6, 3, 16)
        outputs.append(plotOutputter)

    for t in zip_longest(outputs, tee(commands, len(outputs))):
        t[0](t[1])

    if args.plot is not None:
        plt.tight_layout()
        plt.savefig(args.plot)
