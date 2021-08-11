from kivy.config import Config

Config.set('graphics', 'width', '850')
Config.set('graphics', 'height', '850')
from threading import Thread
from kivymd.app import MDApp
from kivy.clock import Clock
from retrieve_rundown_gb import InewsPullSortSaveGB
from retrieve_rundown_lk import InewsPullSortSaveLK
from retrieve_rundown_tm import InewsPullSortSaveTM
from retrieve_rundown_lw import InewsPullSortSaveLW
from retrieve_rundown_cs import InewsPullSortSaveCS
from s3_connection import upload_to_aws
from func_timeout import func_timeout, FunctionTimedOut
from datetime import datetime
from calendar import timegm
import time

class ConsoleApp(MDApp):
    ready = False
    gb_log = ['']
    lk_log = ['']
    tm_log = ['']
    lw_log = ['']
    cs_log = ['']
    console = None

    automation_switches = {'gb': False,
                           'lk': False,
                           'tm': False,
                           'lw': False,
                           'cs': False}

    automation = {'gb': [{}, {}, {}, {}],
                  'lk': [{}, {}, {}, {}],
                  'tm': [{}, {}, {}, {}],
                  'lw': [{}, {}, {}, {}],
                  'cs': [{}, {}, {}, {}]}

    rundown_switch_dict = {}

    def build(self):
        InewsPullSortSaveGB.app = InewsPullSortSaveLK.app = InewsPullSortSaveTM.app = InewsPullSortSaveLW.app = \
            InewsPullSortSaveCS.app = self

    def rundown_switch(self, switch, rundown, local_dir, export_path, color):
        self.rundown_switch_dict[export_path] = switch
        if switch:
            self.start_process(switch, rundown, local_dir, export_path, color)

    def start_process(self, switch, rundown, local_dir, export_path, color):

        if self.rundown_switch_dict[export_path]:
            t = Thread(target=self.collect_rundown, args=(rundown, local_dir, export_path, color))
            t.daemon = True
            t.start()

    def collect_rundown(self, rundown, local_dir, export_path, color):
        inews_conn = eval('InewsPullSortSave' + export_path[:2].upper())()
        inews_conn.init_process(rundown, local_dir, export_path, color)

        t = Thread(target=self.send_to_aws, args=(rundown, local_dir, export_path, color))
        t.daemon = False
        t.start()

    def determine_frequency(self, production):
        now = datetime.now()
        seconds_midnight = int((now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds())

        if self.automation_switches[production]:

            for d in self.automation[production]:
                if 'start' in d and 'finish' in d:
                    strt_h, strt_m = d['start'].split(':')
                    fin_h, fin_m = d['finish'].split(':')

                    if ((int(strt_h) * 3600) + (int(strt_m) * 60)) < seconds_midnight\
                            < ((int(fin_h) * 3600) + (int(fin_m) * 60)):
                        return int(d['freq'])
                    else:
                        print('ZZ')
                else:
                    print('XX')
                    return 61

            return 3600

        else:
            return int(eval('self.root.ids.repeat_in_seconds_' + production).text)


    def send_to_aws(self, rundown, local_dir, export_path, color):
        repeat_freq = self.determine_frequency(local_dir[8:10])
        # repeat_freq = int(eval('self.root.ids.repeat_in_seconds_' + local_dir[8:10]).text)
        upload_to_aws('exports/pv/' + export_path + '.json', 'rundowns', 'pv/' + export_path)
        upload_to_aws('exports/sv/' + export_path + '.json', 'rundowns', 'sv/' + export_path)
        Clock.schedule_once(lambda dt: self.countdown(repeat_freq, 'repeat', rundown, local_dir, export_path, color), 0)
        self.console_log(local_dir[8:], color + "Uploading json files to AWS[/color]")

    def countdown(self, num, cmd, rundown, local_dir, output, color):
        if cmd == 'repeat':
            if num == 0:
                return self.start_process(self.ready, rundown, local_dir, output, color)
            num -= 1
            Clock.schedule_once(lambda dt: self.countdown(num, 'repeat', rundown, local_dir, output, color), 1)
            if num % 5 == 0:
                self.console_log(local_dir[8:], color + "Repeating  process in " + str(num) + " seconds[/color]")

    def console_log(self, filename, text):
        message = ''
        self.console = eval('self.root.ids.console_' + filename[:2])
        log = eval('self.' + filename[:2] + '_log')

        log.append(text)

        if len(log) > 7:
            log = log[-10:]

        for i in log:
            message += i + '\n'

        self.console.text = message


if __name__ == '__main__':
    ConsoleApp().run()
