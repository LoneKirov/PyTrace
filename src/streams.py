if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    LOGGER = logging.getLogger(__name__)

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
    from command_statistics import commandsToStats, commandsToStatCSV, Time, LBA, Length, Stream, CommandType
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

    statFields = [Time(True), LBA(), Length(), CommandType(), Stream('stream10'), Stream('stream100'), Stream('stream1000'), Stream('stream10000'), Stream('stream100000')]

    if args.csv is not None:
        outputs.append(lambda cmds: commandsToStatCSV(args.csv, cmds, fields=statFields))

    import matplotlib.pyplot as plt
    if args.plot is not None:
        from os.path import splitext
        name, ext = splitext(args.plot)
        def plotOutputter(cmds):
            from matplotlib.ticker import FormatStrFormatter
            from functools import reduce
            from collections import defaultdict
            from numpy import fromiter,array
            import gc

            def plot(fig, plotFn, customFmt=lambda ax: None, subplot=(1,1,1), title=None, xlabel=None, ylabel=None):
                LOGGER.info('Plotting %s' % title)
                ax = fig.add_subplot(subplot[0], subplot[1], subplot[2])
                ax.legend_ = None
                if title is not None:
                    ax.set_title(title, fontsize=5)
                if xlabel is not None:
                    ax.set_xlabel(xlabel, fontsize=5)
                if ylabel is not None:
                    ax.set_ylabel(ylabel, fontsize=5)
                plotFn(ax)
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
                t = s['Start Time']
                lba = s['LBA']
                if s['Cmd'] is 'R':
                    accum['rLBA'].append(lba)
                    accum['rTime'].append(t)
                elif s['Cmd'] is 'W':
                    accum['wLBA'].append(lba)
                    accum['wTime'].append(t)
                for k, _ in streamKeys.items():
                    accum[k][s[k]].append(s['Start Time'])
                return accum

            accum = { k : defaultdict(list) for k, _ in streamKeys.items() }
            accum['rLBA'] = list()
            accum['rTime'] = list()
            accum['wLBA'] = list()
            accum['wTime'] = list()
            accum['ID'] = list()
            LOGGER.info('Accumulating')
            streams = reduce(accumulator, commandsToStats(cmds, fields=statFields), accum)
            accum['rLBA'] = array(accum['rLBA'])
            accum['rTime'] = array(accum['rTime'])
            accum['wLBA'] = array(accum['wLBA'])
            accum['wTime'] = array(accum['wTime'])
            accum['ID'] = array(accum['ID'])
            for k in sorted(streamKeys.keys()):
                streams[k] = {i : array(t) for i, t in streams[k].items() }
            gc.collect()
          
            rows = 1
            cols = 3
            n = 1
            def rwPlot(ax):
                ax.plot(streams['rTime'], streams['rLBA'], 'b.', markersize=2.0)
                ax.plot(streams['wTime'], streams['wLBA'], 'r.', markersize=2.0)
            fig = plt.figure()
            plot(fig, rwPlot, lambda ax: ax.yaxis.set_major_formatter(FormatStrFormatter('%d')),
                    subplot=(rows, cols, n), title='LBA versus Time', ylabel='LBA', xlabel='Time (sec)')
            n += 1
            plot(fig, lambda ax: ax.plot(streams['rTime'], streams['rLBA'], 'b.', markersize=2.0),
                    lambda ax: ax.yaxis.set_major_formatter(FormatStrFormatter('%d')),
                    subplot=(rows, cols, n), title='Read LBA versus Time', ylabel='LBA', xlabel='Time (sec)')
            n += 1
            plot(fig, lambda ax: ax.plot(streams['wTime'], streams['wLBA'], 'r.', markersize=2.0),
                    lambda ax: ax.yaxis.set_major_formatter(FormatStrFormatter('%d')),
                    subplot=(rows, cols, n), title='Write LBA versus Time', ylabel='LBA', xlabel='Time (sec)')
            fig.tight_layout()
            fig.savefig('%s-%s%s' % (name, 'lba', ext))
            plt.cla()
            plt.clf()
            plt.close(fig)
            del fig
            del streams['rLBA']
            del streams['rTime']
            del streams['wLBA']
            del streams['wTime']
            for k in sorted(streamKeys.keys()):
                gc.collect()
                fig = plt.figure()
                n = 1
                v = streamKeys[k]
                plot(fig, lambda ax: [ax.plot(t, fromiter([i for _ in range(len(t))], int), '.', markersize=2.0) for i, t in streams[k].items()],
                        subplot=(rows, cols, n), title=str(v) + 'ms Streams', xlabel='Time (sec)', ylabel='Stream ID')
                n += 1
                gc.collect()
                ids = fromiter([ i for i, _ in streams[k].items() if i is not -1 ], int)
                plot(fig, lambda ax: ax.bar(ids, fromiter([ len(t) for i, t in streams[k].items() if i is not -1 ], int)),
                        subplot=(rows, cols, n), title=str(v) + 'ms Stream Lengths (commands)', xlabel='Stream ID',
                        ylabel='Number of Commands')
                n += 1
                gc.collect()
                plot(fig, lambda ax: ax.bar(ids, fromiter([ t[len(t) - 1] - t[0] for i, t in streams[k].items() if i is not -1 ], int)),
                        subplot=(rows, cols, n), title=str(v) + 'ms Stream Lengths (duration)', xlabel='Stream ID',
                        ylabel='Duration (sec)')
                gc.collect()
                del streams[k]
                fig.tight_layout()
                LOGGER.info('Saving %s' % ('%s-%s%s' % (name, k, ext)))
                fig.savefig('%s-%s%s' % (name, k, ext))
                plt.close(fig)
                plt.cla()
                plt.clf()
                del fig
                gc.collect()

        outputs.append(plotOutputter)

    for t in zip_longest(outputs, tee(commands, len(outputs))):
        t[0](t[1])
