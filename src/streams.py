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
    from command_statistics import commandsToStats, commandsToStatCSV, Time, LBA, Length, Stream
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
            from matplotlib.ticker import FormatStrFormatter
            from functools import reduce
            from collections import defaultdict
            from numpy import fromiter

            def plot(plotFn, customFmt=lambda ax: None, subplot=(1,1,1), title=None, xlabel=None, ylabel=None):
                ax = plt.subplot(subplot[0], subplot[1], subplot[2])
                if title is not None:
                    ax.set_title(title, fontsize=5)
                if xlabel is not None:
                    ax.set_xlabel(xlabel, fontsize=5)
                if ylabel is not None:
                    ax.set_ylabel(ylabel, fontsize=5)
                plotFn()
                plt.setp(ax.get_xticklabels(), fontsize=4)
                plt.setp(ax.get_yticklabels(), fontsize=4)
                customFmt(ax)

            streamKeys = {
                'stream10' : 10,
                'stream100' : 100,
                'stream1000' : 1000,
                'stream10000' : 10000,
                'stream100000' : 100000
            }

            def accumulator(accum, s):
                accum['LBA'].append(s['LBA'])
                t = s['Start Time']
                accum['Time'].append(t)
                for k, _ in streamKeys.items():
                    accum[k][s[k]].append(s['Start Time'])
                return accum

            accum = { k : defaultdict(list) for k, _ in streamKeys.items() }
            accum['LBA'] = list()
            accum['Time'] = list()
            accum['ID'] = list()
            streams = reduce(accumulator, commandsToStats(cmds, fields=statFields), accum)
          
            rows = 6
            cols = 3
            n = 1
            plot(lambda: plt.plot(streams['Time'], streams['LBA'], '.', markersize=2.0),
                    lambda ax: ax.yaxis.set_major_formatter(FormatStrFormatter('%d')),
                    subplot=(rows, cols, n), title='LBA versus Time', xlabel='LBA', ylabel='Time (sec)')
            del streams['LBA']
            del streams['Time']
            n = 4
            for k in sorted(streamKeys.keys()):
                v = streamKeys[k]
                plot(lambda: [plt.plot(t, [i for _ in range(len(t))], '.', markersize=2.0) for i, t in streams[k].items()],
                        subplot=(rows, cols, n), title=str(v) + 'ms Streams', xlabel='Time (sec)', ylabel='Stream ID')
                n += 1
                ids = [ i for i, _ in streams[k].items() if i is not -1 ]
                plot(lambda: plt.bar(ids, [ len(t) for i, t in streams[k].items() if i is not -1 ]),
                        subplot=(rows, cols, n), title=str(v) + 'ms Stream Lengths (commands)', xlabel='Stream ID',
                        ylabel='Number of Commands')
                n += 1
                plot(lambda: plt.bar(ids, [ t[len(t) - 1] - t[0] for i, t in streams[k].items() if i is not -1 ]),
                        subplot=(rows, cols, n), title=str(v) + 'ms Stream Lengths (duration)', xlabel='Stream ID',
                        ylabel='Duration (sec)')
                n += 1
                del streams[k]
        outputs.append(plotOutputter)

    for t in zip_longest(outputs, tee(commands, len(outputs))):
        t[0](t[1])

    if args.plot is not None:
        plt.tight_layout()
        plt.savefig(args.plot)
