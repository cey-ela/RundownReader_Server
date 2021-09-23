from kivy.config import Config
Config.set('graphics', 'width', '900')
Config.set('graphics', 'height', '900')
from threading import Thread
from kivymd.app import MDApp
from kivy.clock import Clock
from s3_connection import upload_to_aws
from datetime import datetime
from ftplib import FTP
from inews_pull_sort_push import InewsPullSortPush
import re
from kivy.uix.textinput import TextInput
import json
from kivy import properties as kp


class ConsoleApp(MDApp):
    """
    ..the epicenter...
    """
    gb_log = kp.ListProperty()  # lists used to store the output console text
    lk_log = kp.ListProperty()  # ^
    tm_log = kp.ListProperty()  # ^
    lw_log = kp.ListProperty()  # ^
    cs_log = kp.ListProperty()  # ^
    ftp_sessions = kp.DictProperty()  # Each individual FTP connection is stored here

    def __init__(self):
        super().__init__()
        # Retrieve iNews FTP credentials and IP from secure external source
        with open("C:\\Program Files\\RundownReader_Server\\xyz\\aws_creds.json") as aws_creds:
        #with open("/Users/joseedwa/PycharmProjects/xyz/aws_creds.json") as aws_creds:  # Move these credentials
            inews_details = json.load(aws_creds)
        self.ip = inews_details[1]['ip']
        self.passwd = inews_details[1]['passwd']
        self.user = inews_details[1]['user']

        # interrogated when the countdown ends to see if the process should repeat
        self.repeat_switches = {'gb': False,
                                'lk': False,
                                'tm': False,
                                'lw': False,
                                'cs': False}

        # and if that process should abide by the automation schedule
        self.automation_switches = {'gb': False,
                                    'lk': False,
                                    'tm': False,
                                    'lw': False,
                                    'cs': False}

        with open('schedule.json') as sched:  # Amendable schedule saved in ext .json to survive app restarts
            self.schedule = json.load(sched)

    def build(self):
        """
        Called as the .kv elements are being constructed, post init()
        """
        self.title = 'NEW SERVER'
        InewsPullSortPush.app = self  # how alternate .py files communicate back to the main App class

        # Populate each automation schedule input box. The boxes follow the same layout as schedule.json so both
        # can be 'zipped' through concurrently, correctly transferring data between the two locations
        for prod in self.root.ids:
            if 'auto' in prod:
                for input_box in self.root.ids[prod].ids:
                    self.root.ids[prod].ids[input_box].text = self.schedule[prod[-2:]][input_box]

    def update_schedule(self):
        """
        Precise reverse of the last loop in build(). schedule.json updated when the schedule is manually changed
        """
        for prod in self.root.ids:
            if 'auto' in prod:
                for input_box in self.root.ids[prod].ids:
                    self.schedule[prod[-2:]][input_box] = self.root.ids[prod].ids[input_box].text

        with open('schedule.json', 'w') as f:
            f.write(json.dumps(self.schedule, indent=4))

    def rundown_switch(self, switch, rundown, local_dir, export_path, color):
        """
        Called when the switch in the 'START' column is toggled to start/stop the sequence of methods
        It lets repeat_switches know if the rundown download-sort-upload, should be repeated at the end of the countdown
        starts new FTP session and triggers the next function in the sequence

        note:~ These params are passed down through each method
        :param switch: bool value passed by the state of the switch that triggers this method
        :param rundown: iNews rundown dir path
        :param local_dir: local dir used to temporarily house the FTP downloads
        :param export_path: the AWS dir
        :param color: used to color output console text correctly
        """
        prod = export_path[3:]  # = gb / lk / tm / lw / cs
        self.repeat_switches[prod] = switch  # Update dict used at the end of the countdown repeat - yes/no

        if switch:  # If value from the switch being toggled is True/active/on
            try:  # If session exists, quit
                self.ftp_sessions[prod].quit()
                print('Closing connection: ' + str(self.ftp_sessions[prod]))
            except KeyError:
                pass
            self.ftp_sessions[prod] = FTP(self.ip)  # Start a new FTP session and store it as an object in a dict
            self.ftp_sessions[prod].login(user=self.user, passwd=self.passwd)
            print('Opening new FTP connection: ' + str(self.ftp_sessions[prod]))
            self.collect_rundown_thread(rundown, local_dir, export_path, color)
        else:
            self.console_log(local_dir[8:], color + "Process terminated")

    def collect_rundown_thread(self, rundown, local_dir, export_path, color):
        """
            Threads are used in Kivy to prevent graphical locking of the interface
        """
        if self.repeat_switches[export_path[3:]]:
            t = Thread(target=self.collect_rundown, args=(rundown, local_dir, export_path, color))
            t.daemon = True
            t.start()

    def collect_rundown(self, rundown, local_dir, export_path, color):
        """
        Create a new instance of the iNews process class and initialise the sequence of methods within. See
        inews_pull_sort_push.py for details. Once complete start a thread for the next method
        """
        inews_conn = InewsPullSortPush()
        inews_conn.init_process(rundown, local_dir, export_path, color)

        t = Thread(target=self.push_to_aws, args=(rundown, local_dir, export_path, color))
        t.daemon = False
        t.start()

    def push_to_aws(self, rundown, local_dir, export_path, color):
        """
        Once new pv/sv export files have been created, push to AWS (see s3_connection.py)
        establish how long the next countdown() should take and run it
        """
        upload_to_aws('exports/pv/' + export_path + '.json', 'rundowns', 'pv/' + export_path)
        upload_to_aws('exports/sv/' + export_path + '.json', 'rundowns', 'sv/' + export_path)
        duration = self.determine_frequency(local_dir[8:10])
        self.console_log(local_dir[8:], color + "Uploading json files to AWS[/color]")
        self.countdown(duration, rundown, local_dir, export_path, color)

    def countdown(self, duration, rundown, local_dir, export_path, color):
        """
        The last function in the sequence. Using the duration parameter countdown once a second using schedule_once(1)
        When duration == 0 and if the switch has remained on, repeat the entire process
        :param duration: time remaining
        """
        if duration == 0 and self.repeat_switches[export_path[3:]]:
            return self.collect_rundown_thread(rundown, local_dir, export_path, color)

        duration -= 1
        if self.repeat_switches[export_path[3:]]:  # If start switch hasn't been deactivated
            Clock.schedule_once(lambda dt: self.countdown(duration, rundown, local_dir, export_path, color), 1)
            if duration % 5 == 0:  # Update console every 5 seconds
                self.console_log(local_dir[8:], color + "Repeating  process in " + str(duration) + " seconds[/color]")

    def determine_frequency(self, filename):
        """
        Access the string values (hh:mm) of self.schedule and determine if the current time ((int) seconds from
        midnight) falls between any schedule times. If so, return the return duration associated with the schedule
        period to be used by the next countdown()
        :param filename: received as a slice of the local_dir to extract an identifier, e.g. 'lk', 'tm'
        """
        now = datetime.now()  # determine current seconds from midnight in integer form
        seconds_midnight = int((now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds())

        start_h = 0
        start_m = 0
        fin_h = 0
        fin_m = 0

        if self.automation_switches[filename]:  # If automate switch for the selected production is on:
            for x, y in self.schedule[filename].items():  # Loop dict for each key(x), val(y)
                if 'start' in x:
                    start_h, start_m = (int(x) for x in y.split(':'))  # Store start hour and min
                elif 'fin' in x:
                    fin_h, fin_m = (int(x) for x in y.split(':'))  # Store finish hour and min
                else:
                    freq = int(y)  # Lastly store it's freq, then before moving onto the next start item check if
                    # the current actual time falls between the saved start and finish times:
                    if ((start_h * 3600) + (start_m * 60)) < seconds_midnight < ((fin_h * 3600) + (fin_m * 60)):
                        return freq  # If it does then return the set frequency for that time period and break out loop

            return 3651  # If current actual time fail to fall within any schedules then set default freq to 1 hour

        else:
            # If automate switch is off, use the value from left-hand-side frequency box
            return int(self.root.ids['repeat_in_seconds_' + filename].text)

    def console_log(self, filename, text):
        """
        Update the output console
        :param text: new line of text to display
        :return:
        """
        message = ''  # create var

        log = getattr(self, filename[:2] + '_log')  # define log/list to update

        log.append(text)  # add text received as argument

        if len(log) > 11:  # keep the log fewer than 11 rows
            log.pop(0)

        for i in log:  # build output
            message += i + '\n'

        self.root.ids['console_' + filename[:2]].text = message  # update console

    def on_stop(self):
        """
        Run when the app is closed gracefully
        """
        for sesh in self.ftp_sessions.values():
            sesh.quit()
            print(str(sesh) + ': connection has been closed')
        print('cya')



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
