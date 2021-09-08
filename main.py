from kivy.config import Config

Config.set('graphics', 'width', '850')
Config.set('graphics', 'height', '950')
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
import re
import json
from kivy import properties as kp
from kivy.uix.textinput import TextInput

class ConsoleApp(MDApp):
    ready = False
    gb_log = ['']
    lk_log = ['']
    tm_log = ['']
    lw_log = ['']
    cs_log = ['']
    console = None
    schedule = kp.ObjectProperty()

    automation_switches = {'gb': False,
                           'lk': False,
                           'tm': False,
                           'lw': False,
                           'cs': False}


    rundown_switch_dict = {}

    def build(self):
        InewsPullSortSaveGB.app = InewsPullSortSaveLK.app = InewsPullSortSaveTM.app = InewsPullSortSaveLW.app = \
            InewsPullSortSaveCS.app = self
        self.populate_schedule()

    def populate_schedule(self):
        with open('schedule.json') as sched:
            self.schedule = json.load(sched)

        for prod in self.root.ids:
            if 'auto' in prod:
                for input_box in self.root.ids[prod].ids:
                    self.root.ids[prod].ids[input_box].text = self.schedule[prod[-2:]][input_box]

    def update_schedule(self):
        for prod in self.root.ids:
            if 'auto' in prod:
                for input_box in self.root.ids[prod].ids:
                    self.schedule[prod[-2:]][input_box] = self.root.ids[prod].ids[input_box].text

        with open('schedule.json', 'w') as f:
            f.write(json.dumps(self.schedule, indent=4))

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
        start_h = 0
        start_m = 0
        fin_h = 0
        fin_m = 0

        if self.automation_switches[production]:  # If automate switch for the selected production is on:
            for x, y in self.schedule[production].items():  # Loop dict for each key(x), val(y)
                if 'start' in x:
                    start_h, start_m = (int(x) for x in y.split(':'))   # Store start hour and min
                elif 'fin' in x:
                    fin_h, fin_m = (int(x) for x in y.split(':'))   # Store finish hour and min
                else:
                    freq = int(y)  # Lastly store it's freq, then before moving onto the next start item check if
                    # the current actual time falls between the saved start and finish times:
                    if ((start_h * 3600) + (start_m * 60)) < seconds_midnight < ((fin_h * 3600) + (fin_m * 60)):
                        return freq  # If it does then return the set frequency for that time period and break out loop

            return 3650  # If current actual time fail to fall within any schedules then set default freq to 1 hour

        else:
            # If automate switch is off, use the value from left-hand-side frequency box
            return int(self.root.ids['repeat_in_seconds_' + production].text)


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



class AutoTI(TextInput):

    pat = re.compile('[^0-9]')

    def insert_text(self, substring, from_undo=False):
        pat = self.pat

        if not from_undo and len(self.text) > 4:
            s = substring
            return s
        elif len(self.text) == 1:
            s = re.sub(pat, '', substring) + ':'
        else:
            s = re.sub(pat, '', substring)

        return super(AutoTI, self).insert_text(s, from_undo=from_undo)


if __name__ == '__main__':
    ConsoleApp().run()
