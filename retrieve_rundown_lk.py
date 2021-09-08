# Property Of The British Broadcasting Corporation 2020 - Oliver Mardle.

from ftplib import FTP
from ftplib import all_errors
import re
import datetime
import json
import os
import itertools
from itertools import cycle
from kivy import properties as kp
import time
from func_timeout import func_timeout, FunctionTimedOut
from email_notification import email_error_notification as een


class InewsPullSortSaveLK:
    app = None
    console = None
    epoch_time = int(time.time())
    timeout_counter = 0

    def __init__(self):
        self.story_ids = []  # list of story_id titles in string form. E.g. 'E45RF34'
        self.data = []  # Once converted from XML each story dict gets stored in here

        self.times_list = []  # total time/duration for each story in valid MM:SS, 00:00 if blank
        self.hard_backtimes = []  # hard-out backtimes from iNews in seconds format. E.g. @34256
        self.backtime_pos_list = []  # An indexed list for the above hard-out backtimes, blank positions if no backtime present
        self.backtimes_calculated = []  # Final backtime list. Appended in reverse by subtracting time_list elements from hard_backtimes lst

    def init_process(self, inews_path, local_dir, filename, color):
        # 1ST
        self.app.console_log(filename, color + 'Connecting...[/color]')

        try:
            func_timeout(30, self.pull_xml_via_ftp, args=(inews_path, local_dir, filename, color))

        except FunctionTimedOut:
            self.app.console_log(filename, color + '...RETR cmd hung, retrying...[/color]')
            print('RETR cmd hung, retrying ' + str(datetime.datetime.now()))
            return self.init_process(inews_path, local_dir, filename, color)


        except TimeoutError:
            self.app.console_log(filename, color + '...connection to iNews timeout, check connection...[/color]')
            print('General IP connection failure ' + str(datetime.datetime.now()))
            return self.init_process(inews_path, local_dir, filename, color)

        except all_errors:
            print('FTP - general error ' + str(datetime.datetime.now()))
            self.app.console_log(filename, color + 'FTP error..trying again...[/color]')
            return self.init_process(inews_path, local_dir, filename, color)

        ## 2ND
        self.convert_xml_to_dict(local_dir)
        self.app.console_log(filename, color + "Converting stories from NSML to local dict[/color]")

        # 3RD
        self.backtime_preparation()

        # 4TH
        self.backtimes_calculated_and_set()
        self.app.console_log(filename, color + "Calculating backtimes[/color]")

        # 5TH
        self.finalise()
        self.app.console_log(filename, color + "Cleaning/Finalising data[/color]")

        # 6TH
        self.convert_to_json(filename)
        self.app.console_log(filename, color + "Converting dict to json[/color]")

        # 7th
        self.separate_files(filename)

    def pull_xml_via_ftp(self, inews_path, local_dir, filename, color):
        counter = 0
        # with open("/Users/joseedwa/PycharmProjects/xyz/aws_creds.json") as aws_creds:
        with open("C:\\Program Files\\RundownReader_Server\\xyz\\aws_creds.json") as aws_creds:
            inews_details = json.load(aws_creds)
            user = inews_details[1]['user']
            passwd = inews_details[1]['passwd']
            ip = inews_details[1]['ip']

        # Open FTP connection
        ftp = FTP(ip)
        ftp.login(user=user, passwd=passwd)

        # Retrieve rundown using 'path' parameter passed when function is called
        # e.g. ftp.cwd("CTS.TX.0600")
        ftp.cwd(inews_path)

        # Store story ID as list of titles. E.g. '5AE4RT2'
        self.story_ids = ftp.nlst()

        # Cycles through each line/Story ID and opens as a new file
        # RETRIEVE the contents of each Story ID from iNews and store it in new_story_file hen save to local_dir

        for story_id_title in self.story_ids:
            self.epoch_time = int(time.time())

            with open(local_dir + story_id_title, "wb") as new_story_file:
                ftp.retrbinary("RETR " + story_id_title, new_story_file.write)

            new_story_file.close()
            counter += 1
            self.epoch_time = int(time.time())
            if counter % 25 == 0:
                self.app.console_log(filename, color + str(counter) + ' stories pulled[/color]')

        # Close FTP connection
        ftp.quit()

    def convert_xml_to_dict(self, local_dir):
        # TODO: NOTE - focus, brk, and floated default false values removed, try and work App without these
        """ A bulky method that in summary, takes each raw FTP NSML story file and converts it to a dict.
        Stripping out the bits we don't need and reformatting some dict key/values along the way.
        By the end we are left with a neat list of dicts (self.data)
        NSML (XML) Example:
        <nsml version="-//AVID//DTD NSML 1.0//EN">
        <head>
        3<meta rate=180 float>
        <rgroup number=46></rgroup>
        <wgroup number=18></wgroup>
        <formname>THISMORNINGS_V2</formname>
        7<storyid>259847d2:00ce0ba3:605b6fc5</storyid>
        </head>
        <story>
        <fields>
        <f id=source></f>
        <f id=var-script></f>
        <f id=camera></f>
        <f id=location></f>
        <f id=page-number></f>
        <f id=title>+ PERMA &amp; HASHTAG</f>
        <f id=video-id></f>
        <f id=event-status></f>
        <f id=item-channel></f>
        <f id=sound></f>
        <f id=audio-time>0</f>
        <f id=runs-time>0</f>
        <f id=total-time>0</f>
        <f id=back-time></f>
        <f id=modify-date>1616605125</f>
        <f id=modify-by>barrtho1</f>
        <f id=air-date></f>
        <f id=var-1></f>
        <f id=gfxready></f>
        <f id=gfxprep></f>
        <f id=vartm-01></f>
        </fields>
        We only want some meta data from line 3, story_id form line 7 and then everything between /fields.
        """

        # Cycle through and open each newly created story file
        for story_id_title in self.story_ids:
            break_out_flag = False
            with open(local_dir + story_id_title, "rb") as story_file:

                # File will now have relevant contents stripped, sanitised and placed into this dict:
                story_dict = {}

                # We only want the data between XML stages <fields> and </fields>. Copy is set to false outside of that
                copy = False

                # Loop through each line in the XML story file and extract necessary info into dict
                for line in story_file:
                    # Checks if 'float' is in 'meta' line of story at the top of the file. Plus it decodes line from
                    # bytes and strips off any new line characters that may be present
                    # TODO: refactor these float and break keys to not have elif/False outcomes
                    if "float" in (line.decode()).strip() and "<meta" in (line.decode()).strip():
                        # If True it adds 'floated' key to 'storyLine' dictionary and sets its value it value to True
                        break_out_flag = True
                        break
                    #     story_dict["floated"] = True
                    #     # OMIT?
                    # elif "float" not in (line.decode()).strip() and "<meta" in (line.decode()).strip():
                    #     # Set 'floated' to False if so
                    #     story_dict["floated"] = False

                    # Check if 'break' is in 'meta' line of story.
                    if "break" in (line.decode()).strip() and "<meta" in (line.decode()).strip():
                        story_dict["brk"] = True
                        # OMIT?
                    elif "break" not in (line.decode()).strip() and "<meta" in (line.decode()).strip():
                        # False if not
                        story_dict["brk"] = False

                    # Extract the first part of story id to be used as a unique ID in the app
                    if "<storyid>" in (line.decode()).strip():
                        result = re.search('<storyid>(.*)</storyid>', (line.decode()).strip())
                        sep = ':'
                        stripped = result.group(1).split(sep, 1)[0]

                        story_dict["story_id"] = stripped

                    if (line.decode()).strip() == "<fields>":
                        # If found, copy = True. Copy controls whats added to the storyLine dictionary. In this case
                        # Everything between "<fields>", line by line until we reach "</fields>"
                        copy = True
                        # OMIT?
                        continue

                    elif (line.decode()).strip() == "</fields>":
                        copy = False
                        continue

                    # Depending on what is decided above, the line is cleaned and copied - or ignored
                    if copy:

                        # Decode line from bytes and strips off any new line characters
                        decoded_line = (line.decode()).strip()

                        # New variable which is a regular expression used to pull ID & data from decoded_line.
                        # Simply put, it's making 2 groups. Group#1 is the string between 'id=' and '>', group#2 between
                        # '>' and '</f>'
                        entry = re.search("<f id=(.*)>(.*)</f>", decoded_line)

                        # These become key value pairs for 'story_dict'
                        key = entry.group(1)
                        value = entry.group(2)

                        # Now for some necessary formatting of some key/values
                        if "total-time" in key:
                            key = 'totaltime'
                            story_dict[key] = value

                        if "back-time" in key:
                            key = "backtime"
                            story_dict[key] = value

                        if "page-number" in key:
                            key = "page"
                            story_dict[key] = value

                        if key == "title":
                            if len(value) >= 50:
                                value = (value[:50] + "...")

                        if key == "format":
                            if len(value) >= 20:
                                value = (value[:20] + "...")

                        if 'amp;' in value:
                            value = value.replace('amp;', '')

                        if 'gt;' in value:
                            value = value.replace('gt;', '')

                        # In any time values present (excluding complex backtime), convert value to MM:SS format
                        # TOTALTIME is importantly converted here
                        if "time" in key and key != "backtime":
                            try:
                                story_dict[key] = datetime.datetime.fromtimestamp(int(value)).strftime("%M:%S")
                            except ValueError:
                                story_dict[key] = ""

                        # Else write key and value as they are
                        else:
                            story_dict[key] = value

                if not break_out_flag:
                    # Append story_dict to 'data' list
                    self.data.append(story_dict)

                # Close story file
                story_file.close()

                # 8) Deletes the file we just read as it's no longer needed
                os.remove(local_dir + story_id_title)

    # ## ### #### TIMINGS TIMINGS TIMINGS TIMINGS #### ### ## #
    # ## ### #### TIMINGS TIMINGS TIMINGS TIMINGS #### ### ## #
    # ## ### #### TIMINGS TIMINGS TIMINGS TIMINGS #### ### ## #
    # Now for the complicated task of filling in the crucial 'backtime' field of iNews.
    # The only time data that comes in from FTP is an occasional hard-out time and the total time of each line - so in
    # order for each line to have a populated backtime field we need to run some calculations.
    # The best way to visualise how we do this is to consider two columns: TOTAL and BACKTIME, we set
    # the last hard-out time into the last backtime field at the bottom of the rundown, working in reverse we then
    # subtract each total time from the previous backtime to populate the current backtime field.
    #
    # It is complicated and involves reversing the order of the list a couple of times along with
    # converting to and from seconds to hours:minutes:secs format - but so far it seems robust and it's producing
    # consistently correct backtimes.

    # times_list = []  # total time/duration for each story in valid MM:SS, 00:00 if blank
    # hard_backtimes = []  # hard-out backtimes from iNews in seconds format. E.g. @34256
    # backtime_pos_list = []  # An indexed list for the above hard-out backtimes, blank positions if no backtime present
    # backtimes_calculated = []  # Final backtime list. Appended in reverse by subtracting time_list elements from hard_backtimes lst

    def backtime_preparation(self):
        for story_dict in self.data:
            # Sometimes the TM rundown doesn't have a total-time field, assign a blank one
            if not 'totaltime' in story_dict:
                story_dict['totaltime'] = ""

            # If backtime exists, 0 that story's totaltime/duration. Not needed and if present can cause timing bugs
            if story_dict['backtime'] != "":
                story_dict['totaltime'] = ""

            # Give floated stories or blank totaltime's a readable MM:SS format in the TIMES list, or append true time
            if story_dict['totaltime'] == "":
                self.times_list.append("00:00")
            else:
                self.times_list.append(story_dict['totaltime'])

            # If backtime is empty, append to backtime_position list
            if story_dict['backtime'] == "":
                # Append empty string
                self.backtime_pos_list.append("")

            # If backtime is not an empty string, append to backtime list and backtime_position list
            # MAKE ELSE?
            if story_dict['backtime'] != "":
                self.hard_backtimes.append(story_dict["backtime"])
                self.backtime_pos_list.append(story_dict["backtime"])

    # Each dict in sorted list contributes either a 00:00 or valid MM:SS to TIMES list
    # It also contributes either a blank "" or valid backtime (in seconds) to BACKTIME_POS
    def backtimes_calculated_and_set(self):
        # The last time from the backtime list is retrieved
        if self.hard_backtimes:
            current_time = int(self.hard_backtimes[-1].strip('@'))
        else:
            current_time = 0

        # get_back_times only becomes true when a hard coded backtime is found in the backtime_position list
        get_back_times = False

        # This for loop counts through all of the lists and adds up the backtimes
        # REM 0?
        for x in range(0, len(self.times_list)):

            # Cycle BACKTIME_POS list in reverse looking for valid BACKTIME'S
            if list(reversed(self.backtime_pos_list))[x] != "":
                get_back_times = True

            if get_back_times is True:
                # backtime_pos_list index position decided by:
                # (numerical length of list - current index pos(x) - 1(to offset index start at 0, list from 1)
                # If '@' is present at this position it indicates a hardcoded backtime. E.g. @34560
                if "@" in self.backtime_pos_list[len(self.backtime_pos_list) - x - 1]:
                    # Strip '@', convert to int seconds. CHANGE CURRENT_TIME
                    current_time = int(self.backtime_pos_list[len(self.backtime_pos_list) - x - 1].strip('@'))

                # At each index iteration of TIMES_LIST convert times(MM:SS) to CURRENT_SECONDS
                t = (self.times_list[len(self.times_list) - x - 1])
                m, s = t.split(':')
                current_seconds = (int(datetime.timedelta(minutes=int(m), seconds=int(s)).total_seconds()))

                # FINALLY - current time -= current_seconds, from TIME_LIST, most frequently 0 but occasionally
                # chips away
                current_time -= int(current_seconds)

                # Append to  NEW backtimes list with UPDATED CURRENT_TIME, converted to hours, minutes, seconds
                self.backtimes_calculated.append(str(datetime.timedelta(seconds=current_time)))
            else:
                # Else append empty string
                self.backtimes_calculated.append("")

        # Flip backtimes list
        self.backtimes_calculated = list(reversed(self.backtimes_calculated))

    def finalise(self):
        """Some final tidying includes: """

        for index, story_dict in enumerate(self.data):

            # Assign updated backtimes to each story
            if story_dict['backtime'] == "":
                story_dict['backtime'] = self.backtimes_calculated[self.data.index(story_dict)]

            # Convert the hard backtime's to HH:MM:SS from seconds
            if "@" in story_dict["backtime"]:
                story_dict["backtime"] = str(datetime.timedelta(seconds=int(story_dict["backtime"].strip('@'))))



            try:
                # Alongside the HH:MM:SS backtime there is a seconds from midnight key/value
                if story_dict["backtime"]:

                    story_dict["seconds"] = sum(x * int(t) for x, t in
                                                zip([3600, 60, 1], story_dict["backtime"].split(":")))

                else:
                    story_dict["seconds"] = self.data[index - 1]["seconds"]

            except KeyError:
                story_dict["seconds"] = 0

            story_dict['focus'] = False


        # Rundown can be divided into Item sections. E.g. 1., 2., 3... This is used to skip through the list in
        # scrollview setting up swipe between items when in page view
        # Equation of a straight line (y=mx+b). See more here: https://www.mathsisfun.com/equation_of_line.html

        # intercepts
        b_pos = -0.0095
        b_neg = -0.0105

        # slope
        m = 0.001 / 0.05
        for index, story_dict in enumerate(reversed(self.data)):

            if story_dict['page'] and story_dict['page'][-2:] == '00':

                pos = (index / len(self.data))

                if 0.5 <= pos < 0.98:
                    offset = round(round(m * pos + b_pos, 10), 3)
                    pos += abs(offset)
                elif 0.02 <= pos < 0.5:
                    offset = round(round(m * pos + b_neg, 10), 3)
                    pos -= abs(offset)

                story_dict['pos'] = round(pos, 3)
            else:
                story_dict['pos'] = None

    def convert_to_json(self, filename):
        """...Export..."""

        with open('exports/sv/' + filename + '.json', 'w') as outfile:
            outfile.write(json.dumps(self.data, indent=4, sort_keys=True))

    def separate_files(self, filename):
        # Gather the titles indexes and title titles
        # slices = [0]
        # new_dict_titles = ['S']
        # for index, story_dict in enumerate(self.data):
        #     if story_dict['page'] and story_dict['page'][-1] == '.':
        #         slices.append(index)
        #         new_dict_titles.append(story_dict['page'])
        #
        # newer_dicts = {}
        # # Using the current and next index from slice, attempt to store chunks
        # # of self.data in new_dicts
        # for a, b, title in zip(slices, slices[1:] + [slices[0]], new_dict_titles):
        #     print(title)
        #     newer_dicts[title] = self.data[a:b-1]
        #
        # with open('new_test.json', 'w') as outfile:
        #     outfile.write(json.dumps(newer_dicts, indent=4))

        slices = [0]

        for index, story_dict in enumerate(self.data):
            if 'tm' in filename or 'lw' in filename:
                if story_dict['page'] and story_dict['page'][-1] == '.':
                    slices.append(index)
            elif 'lk' in filename:
                if story_dict['page'][-2:] == '00':
                    slices.append(index)


        newer_dicts = []
        # Using the current and next index from slice, attempt to store chunks
        # of self.data in new_dicts
        for a, b in zip(slices, slices[1:] + [slices[0]]):
            newer_dicts.append(self.data[a:b])

        if not newer_dicts[-1]:
            newer_dicts.pop()



        with open('exports/pv/' + filename + '.json', 'w') as outfile:
            outfile.write(json.dumps(newer_dicts, indent=4))



#inews = InewsDataPull()
#a inews.init_process("*TM.*OUTPUT.RUNORDERS.TUESDAY.RUNORDER", "stories/tm/wed/", "test_rundown")
#inews.init_process("*GMB-LK.*GMB.TX.0600", "stories/tm/mon/", "exports/gmb_0600")

# generate_json("CTS.TX.0600", "0600")
# generate_json("CTS.TX.0630", "0630")
# generate_json("CTS.TX.0700", "0700")
# generate_json("CTS.TX.0800", "0800")
# generate_json("CTS.TX.TC3_TM", "test_rundown")
# generate_json("CTS.TX.TC2_LW", "test_rundown")
# generate_json("CTS.TX.TC3_GMB", "test_rundown")

# generate_json("*GMB-LK.*GMB.TX.0600", "0600")
# generate_json("*GMB-LK.*GMB.TX.0630", "0630")
# generate_json("*GMB-LK.*GMB.TX.0700", "0700")
# generate_json("*GMB-LK.*GMB.TX.0800", "0800")
# generate_json("*GMB-LK.*LK.TX.LORRAINE", "test_rundown")
# generate_json("*TM.*OUTPUT.RUNORDERS.THURSDAY.RUNORDER", "stories/tm/thu/", "test_rundown")
# generate_json("*LW.RUNORDERS.THURSDAY", "test_rundown")
