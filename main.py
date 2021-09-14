from kivy.config import Config
Config.set('graphics', 'width', '850')
Config.set('graphics', 'height', '950')
from threading import Thread
from kivymd.app import MDApp
from kivy.clock import Clock
from s3_connection import upload_to_aws
from datetime import datetime

from ftplib import FTP
from inews_pull_sort_save import InewsPullSortSave
import re
from kivy.uix.textinput import TextInput
import json
from kivy import properties as kp


class ConsoleApp(MDApp):
    ready = False
    gb_log = ['']
    lk_log = ['']
    tm_log = ['']
    lw_log = ['']
    cs_log = ['']
    console = None
    schedule = kp.ObjectProperty()

    repeat_switches = {}

    # interrogated when the countdown ends to see if the process should repeat
    automation_switches = {'gb': False,
                           'lk': False,
                           'tm': False,
                           'lw': False,
                           'cs': False}

    ftp_sessions = {}



    def __init__(self, **kwargs):
        super().__init__()
        with open("/Users/joseedwa/PycharmProjects/xyz/aws_creds.json") as aws_creds:
        #with open("C:\\Program Files\\RundownReader_Server\\xyz\\aws_creds.json") as aws_creds:
            inews_details = json.load(aws_creds)
        self.ip = inews_details[1]['ip']
        self.passwd = inews_details[1]['passwd']
        self.user = inews_details[1]['user']
        # self.ftp_sessions = {'gb': FTP(self.ip),
        #                      'lk': FTP(self.ip),
        #                      'tm': FTP(self.ip),
        #                      'lw': FTP(self.ip),
        #                      'cs': FTP(self.ip)}

    def on_start(self):
        """Called just before the App starts, but after init() and build()"""
        InewsPullSortSave.app = self
        self.populate_schedule()

    def on_stop(self):
        """Cleanly close FTP conns when the program is closed"""
        # for sesh in self.ftp_sessions.values():
        #     sesh.quit()
        pass



    def populate_schedule(self):
        """Upon init, loop through saved schedules and write them to the automation Text Input boxes.
        The .json file copies the layout of the boxes so they can be transferred easily in order"""
        with open('schedule.json') as sched:
            self.schedule = json.load(sched)

        for prod in self.root.ids:
            if 'auto' in prod:
                for input_box in self.root.ids[prod].ids:
                    self.root.ids[prod].ids[input_box].text = self.schedule[prod[-2:]][input_box]

    def update_schedule(self):
        """Precise reverse of populate_schedule. .json updated when the schedule is manually changed"""
        for prod in self.root.ids:
            if 'auto' in prod:
                for input_box in self.root.ids[prod].ids:
                    self.schedule[prod[-2:]][input_box] = self.root.ids[prod].ids[input_box].text

        with open('schedule.json', 'w') as f:
            f.write(json.dumps(self.schedule, indent=4))



    def rundown_switch(self, switch, rundown, local_dir, export_path, color):
        prod = export_path[3:]

        if switch:
            self.ftp_sessions[prod] = FTP(self.ip)
            self.ftp_sessions[prod].login(user=self.user, passwd=self.passwd)
        else:
            self.ftp_sessions[prod].quit()
            self.ftp_sessions.pop(prod)

        self.repeat_switches[export_path] = switch  # Update the on/off dict,
        if switch:
            self.start_process(switch, rundown, local_dir, export_path, color)



    def start_process(self, switch, rundown, local_dir, export_path, color):


        if self.repeat_switches[export_path]:
            t = Thread(target=self.collect_rundown, args=(rundown, local_dir, export_path, color))
            t.daemon = True
            t.start()




    def collect_rundown(self, rundown, local_dir, export_path, color):
        print(rundown)
        print(local_dir)
        print(export_path)

        inews_conn = InewsPullSortSave()
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


        if self.automation_switches[production]:
            print(self.schedule)

            for d in self.schedule[production]:
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
                    return 3600


            return 3651  # If current actual time fail to fall within any schedules then set default freq to 1 hour

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
