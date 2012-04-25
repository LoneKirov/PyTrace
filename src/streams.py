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

    statFields = [Time(True), LBA(), Length(), Stream('stream10'), Stream('stream100'), Stream('stream1000'), Stream('stream10000'), Stream('stream100000')]

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
                ax = plt.subplot(rows, cols, ndx)
                dur = key.replace('stream', '')
                ax.set_title(dur + ' (ms) Streams', fontsize=5)
                ax.set_ylabel('Stream ID', fontsize=5)
                ax.set_xlabel('Time (sec)', fontsize=5)
                for i, t in streams.items():
                    plt.plot(t, [i for _ in range(len(t))], '.', markersize=2.0)
                plt.setp(ax.get_xticklabels(), fontsize=4)
                plt.setp(ax.get_yticklabels(), fontsize=4)
                del streams[-1]
                ax = plt.subplot(rows, cols, ndx + 1)
                ax.set_title(dur + ' (ms) Stream Lengths (commands)', fontsize=5)
                ax.set_ylabel('Number of Commands', fontsize=5)
                ax.set_xlabel('Stream ID', fontsize=5)
                plt.bar([ k for k, _ in streams.items() ], [ len(t) for _, t in streams.items() ])
                plt.setp(ax.get_xticklabels(), fontsize=4)
                plt.setp(ax.get_yticklabels(), fontsize=4)
                ax = plt.subplot(rows, cols, ndx + 2)
                ax.set_title(dur + ' (ms) Stream Durations', fontsize=5)
                ax.set_ylabel('Duration (sec)', fontsize=5)
                ax.set_xlabel('Stream ID', fontsize=5)
                plt.bar([ k for k, _ in streams.items() ], [ t[len(t) - 1] - t[0] for _, t in streams.items() ])
                plt.setp(ax.get_xticklabels(), fontsize=4)
                plt.setp(ax.get_yticklabels(), fontsize=4)
            stats = tee(commandsToStats(cmds, fields=statFields), 7)
            ndx = 4
            for v in zip_longest(stats, ['stream10', 'stream100', 'stream1000', 'stream10000', 'stream100000']):
                if v[1] is not None:
                    plot(v[0], v[1], 6, 3, ndx)
                    ndx += 3
            ax = plt.subplot(6, 3, 1)
            ax.set_ylabel('LBA', fontsize=5)
            from matplotlib.ticker import FormatStrFormatter
            ax.yaxis.set_major_formatter(FormatStrFormatter('%d'))
            ax.set_xlabel('Time (sec)', fontsize=5)
            plt.setp(ax.get_xticklabels(), fontsize=4)
            plt.setp(ax.get_yticklabels(), fontsize=4)
            plt.plot([s['Start Time'] for s in stats[7]], [s['LBA'] for s in stats[6]], '.', markersize=2.0)
        outputs.append(plotOutputter)

    for t in zip_longest(outputs, tee(commands, len(outputs))):
        t[0](t[1])

    if args.plot is not None:
        plt.tight_layout()
        plt.savefig(args.plot)
