# Property Of The British Broadcasting Corporation 2020 - Oliver Mardle.
import re
import datetime
import json
import os
import glob
import time
import ftplib as ftp
from bs4 import BeautifulSoup
from func_timeout import func_timeout, FunctionTimedOut
from kivy.clock import Clock
from email_notification import EmailNotify


class InewsPullSortPush:
    """
    Pull it...sort it...push it...bop it!!
    """
    app = None

    def __init__(self):
        super().__init__()

        self.epoch_time = int(time.time())
        self.data = []
        self.story_ids = []
        self.data_pv = []
        self.error_count = 0
        self.error_limit = 5
        self.email = EmailNotify()

    def init_process(self, inews_path, local_dir, export_path, color):
        """A sequential list of methods to be carried out in order. An instance of this class is called every time
        the repeat countdown reaches 0. It is permitted n(error_limit) connection timeouts or other errors before
        shutting down all connections as a matter of safety. Email notifications will be sent"""

        self.app.console_log(export_path, color + 'Starting rundown download[/color]')

        # 1ST METHOD: Retrieve XML data from iNews via FTP
        try:  # Kick off the iNews data pull via a special func_timeout method: If n seconds are exceeded
            # FunctionTimedOut exception is raised
            func_timeout(45, self.pull_xml_via_ftp, args=(inews_path, local_dir, export_path, color))

        except FileNotFoundError:  # Rundown is empty, early exit/return back to main countdown/repeat
            print(str(datetime.datetime.now())[:19] + ' ~ Rundown empty - Skipping AWS push')
            self.app.console_log(export_path, color + 'Rundown empty. No upload[/color]')
            return

        except (FunctionTimedOut, TimeoutError, OSError) as e:
            # Broad range of Timeout Errors to catch conn issues
            self.app.console_log(export_path, color + ' Connection error - see Python Terminal[/color]')
            print(str(datetime.datetime.now())[:19] + ' ~ Exception:\n' + str(e))
            self.error_count += 1

            if self.error_count <= self.error_limit:
                self.app.inews_disconnect(local_dir, export_path, color)
                print(str(datetime.datetime.now())[:19] + ' ~ Retrying in 15 seconds. Attempt ' +
                      str(self.error_count) + '/5...\n')
                time.sleep(15)
                self.app.inews_connect(local_dir, export_path, color)

                return self.init_process(inews_path, local_dir, export_path, color)  # Retry the process

            else:  # If error cap is reached. Gracefully shut down.
                self.email.email_error_notification('Error cap of 5 has been exceeded. Shutting down. \n'
                                                    'Please investigate and restart software when ready.')
                self.app.console_log(export_path,
                                     color + 'Error limit reached. Processes stopped. See terminal[/color]')
                print('Error cap of 5 has been exceeded. Shutting down. \n'
                      'Please investigate and restart software when ready.')
                for sesh in self.app.ftp_sessions.values():
                    sesh.quit()
                for switch in self.app.manual_switches:
                    self.app.manual_switches[switch] = False

                return  # return to repeat countdown. It will safely fail on repeat as FTP conns are closed

        # except ftp.error_reply:  # If the process times out during the RETR pull, you may see this error
        #     # on its next retry. Simply rerunning will not work. Exit FTP connection and reestablish
        #     self.email.email_error_notification('Timeout during previous pull has expired the FTP session')
        #     self.app.inews_disconnect(local_dir, export_path, color)
        #     print('Waiting 15 seconds until retry...')
        #     self.app.console_log(export_path, color + ' iNews error, see terminal for info[/color]')
        #     time.sleep(15)
        #     self.app.inews_connect(local_dir, export_path, color)

        if self.error_count < self.error_limit:

            # 2ND METHOD: Convert downloaded iNews data into Python dict
            try:  # If timeout error is encountered, depending on the precise time the proceeding method, convert_xml...
                # may be called without any data to work with. This catches that and returns back to repeat countdown
                self.convert_xml_to_dict(local_dir)
                self.app.console_log(export_path, color + "Converting from NSML to dict[/color]")
            except FileNotFoundError:
                return

            # 3RD METHOD: Calculate and set own timings for the show based on raw data
            self.set_backtimes()
            self.app.console_log(export_path, color + "Calculating backtimes[/color]")

            # 4TH METHOD: Any final touches, like adding extra key/val to the dict
            self.finishing_touches()

            # 5TH METHOD: Create another version of the data for Page View
            self.create_pv_version(export_path)

            # 6TH METHOD: Convert dict to .json in preparation for push to AWS
            self.create_json_files(export_path)
            self.app.console_log(export_path, color + "Converting dict to json[/color]")

    def pull_xml_via_ftp(self, inews_path, local_dir, export_path, color):

        """Get data from iNews via FTP"""
        counter = 0  # Used to display amount of files in the output console

        ftp_sesh = self.app.ftp_sessions[export_path[3:5]]  # Select correct FTP session from session list

        ftp_sesh.cwd(inews_path)  # cwd = current working dir. Aka set iNews rundown

        self.story_ids = ftp_sesh.nlst()  # Retrieve list of story names/story_ids in rundown and store in local list

        if not self.story_ids:  # If rundown empty, clean early exit to reenter countdown procedure
            raise FileNotFoundError

        # Cycles through each line/Story ID and opens as a new file
        # RETRIEVE the contents of each Story ID from iNews and store it in new_story_file hen save to local_dir
        for story_id_title in self.story_ids:

            try:
                with open(local_dir + story_id_title, "wb") as new_story_file:
                    ftp_sesh.retrbinary("RETR " + story_id_title, new_story_file.write)
                    # new_story_file.close()

            except (ftp.error_perm, EOFError, AttributeError):  # Permanent error - all actions will have to cease and
                # the ftp conn reestablished. Raises vague but final error_perm exception
                # new_story_file.close()
                continue

            counter += 1
            # if counter % 25 == 0:
            self.app.console_log(export_path, color + str(counter) + ' stories pulled[/color]')

    def convert_xml_to_dict(self, local_dir):
        # TODO: NOTE - focus, brk, and floated default false values removed, try and work App without these
        """ A bulky method that in summary, takes each raw FTP NSML story file and converts it to a dict.
        Stripping out the bits we don't need and reformatting some dict key/values along the way.
        By the end we are left with a neat list of dicts (self.data)
        NSML (XML) Example:

        <nsml version="-//AVID//DTD NSML 1.0//EN">
        <head>
        <meta words=57 rate=180>
        <rgroup number=23></rgroup>
        <wgroup number=14></wgroup>
        <formname>LW-STORY-GEN</formname>
        <storyid>066f5a95:00242d79:61895a16</storyid>
        </head>
        <story>
        <fields>
        <f id=page-number></f>
        <f id=done></f>
        <f id=rewrite></f>
        <f id=title>** OPENING SEQUENCE</f>
        <f id=camera>CAM1</f>
        <f id=video-id></f>
        <f id=pres1> </f>
        <f id=pres2></f>
        <f id=format>M/S -&gt; M/CU RUTH</f>
        <f id=audio-time>19</f>
        <f id=runs-time>0</f>
        <f id=total-time uec>60</f>
        <f id=back-time></f>
        <f id=event-status></f>
        <f id=item-channel>*!</f>
        <f id=grfx> </f>
        <f id=edit></f>
        <f id=painfo></f>
        <f id=create-date>1636025762</f>
        <f id=create-by>richjack</f>
        <f id=modify-date>1636391446</f>
        <f id=modify-by>darigard</f>
        <f id=air-date></f>
        <f id=copyright></f>
        <f id=timecode></f>
        <f id=important></f>
        <f id=channel></f>
        <f id=sound></f>
        <f id=grams></f>
        <f id=var-3></f>
        </fields>
        <body>
        <p><a idref=0> <pi><b>RUTH</b></pi></p>
        <p>Hello and welcome to your Tuesday Loose Women. </p>
        <p>Joining me Ruth Langsford this Tuesday lunchtime, it's:</p>
        <p><cc><b>CAM2: KELLE</b></cc></p>
        <p>Kelle Bryan,</p>
        <p><cc><b>CAM3: JANET</b></cc></p>
        <p>Janet Street-Porter</p>
        <p><cc><b>CAM4: JANE</b></cc></p>
        <p>and Jane Moore. </p>
        <p></p>
        <p><pi><b>WIPE</b></pi></p>
        <p> Coming up, </p>
        <p><a idref=1><cc><b>COMING UP STRAP:  </b></cc></p>
        <p><cc><b>ULAY: </b></cc></p>
        <p></p>
        <p><pi><b>**PAUSE FOR SOT**</b></pi></p>
        <p><pi><b>L WIPE </b></pi></p>
        <p><a idref=2><pi><b>SOT ULAY: XXXX</b></pi></p>
        <p><pi><b>Dip sound at xx secs</b></pi></p>
        <p><cc><b><i>''TRANSCRIBE SOT HERE'' </i></b></cc></p>
        <p><a idref=3><cc><b>COMING UP STRAP:  </b></cc></p>
        <p></p>
        <p>And we'll be going ringside with Paris Fury,</p>
        <p><cc><b>ULAY: PARIS &amp; TYSON</b></cc></p>
        <p><a idref=4><cc><b>COMING UP STRAP: </b></cc></p>
        <p><cc><b>STILLS: PARIS &amp; TYSON, PARIS &amp; FAMILY STILLS</b></cc></p>
        <p>as she opens up about the magic and mayhem of life with a world champion boxer, and how she keeps her marriage and family strong. </p>
        <p></p>
        <p><pi><b>ANIMATE OFF </b></pi></p>
        <p><pi><b>READ ON</b></pi></p>
        <p>.</p>
        <p></p>
        </body>
        <aeset>
        <ae id=0>
        <ap>]] S3.0 G 0 [[</ap>
        <ap></ap>
        </ae>
        <ae id=1>
        <ap>]] S3.0 M 0 [[</ap>
        <mc>
        <ap>maestro ]] 80 YNYAM 5 [[ [A] (LW 2020 COMING_UP_CORNER_NO WIPE) /QWQWQWQW/12.45pm/1</ap>
        </mc>
        </ae>
        <ae id=2>
        <ap>]] S3.0 G 0 [[</ap>
        <ap>XXXX SOT ULAY</ap>
        <ap>DIP AT XX SECS</ap>
        <ap>CLIP ID:</ap>
        <ap>IQ:</ap>
        <ap>OQ:</ap>
        <ap>SOT RUNS=</ap>
        <ap>FULL CLIP RUNS </ap>
        <ap>XXXX</ap>
        </ae>
        <ae id=3>
        <ap>]] S3.0 M 0 [[</ap>
        <mc>
        <ap>maestro ]] 83 YNYAM 5 [[ [A] (LW 2020 COMING_UP_CORNER_NO WIPE) /QWQWQWQW/12.45pm/1</ap>
        </mc>
        </ae>
        <ae id=4>
        <ap>]] S3.0 M 0 [[</ap>
        <mc>
        <ap>maestro ]] 84 YNYAM 5 [[ [A] (LW 2020 COMING_UP_CORNER_NO WIPE) /QWQWQWQW/12.45pm/1</ap>
        </mc>
        </ae>
        </aeset>
        </story>





        """

        # Cycle through and open each newly created story file
        for story_id_title in self.story_ids:
            break_out_flag = False  # If story is floated, set this to true to prevent dict from being added to list
            with open(local_dir + story_id_title, "rb") as story_file:

                # File will now have relevant contents stripped, sanitised and placed into this dict:
                story_dict = {}

                # We only want the data between XML stages <fields> and </fields>. Copy is set to false outside of that
                copy_fields = False
                copy_body = False

                story_dict['focus'] = False
                story_dict['body'] = ''

                # Loop through each line in the XML story file and extract necessary info into dict
                for line in story_file:
                    line = (line.decode()).strip()
                    # Checks if 'float' is in 'meta' line of story at the top of the file. Plus it decodes line from
                    # bytes and strips off any new line characters that may be present
                    # TODO: refactor these float and break keys to not have elif/False outcomes

                    if "float" in line and "<meta" in line:
                        # If True it adds 'floated' key to 'storyLine' dictionary and sets its value it value to True
                        break_out_flag = True
                        break

                    # Check if 'break' is in 'meta' line of story.
                    if "break" in line and "<meta" in line:
                        story_dict["brk"] = True
                        # OMIT?
                    elif "break" not in line and "<meta" in line:
                        # False if not
                        story_dict["brk"] = False

                    # Extract the first part of story id to be used as a unique ID in the app
                    if "<storyid>" in line:
                        result = re.search('<storyid>(.*)</storyid>', line)
                        sep = ':'
                        stripped = result.group(1).split(sep, 1)[0]

                        story_dict["story_id"] = stripped

                    if line == "<fields>":
                        # If found, copy_fields = True. Copy controls whats added to the storyLine dictionary. In this case
                        # Everything between "<fields>", line by line until we reach "</fields>"
                        copy_fields = True
                        continue

                    elif line == "</fields>":
                        copy_fields = False
                        continue

                    elif line == "<body>":
                        copy_body = True
                        continue

                    elif line == "</body>":
                        copy_body = False
                        continue

                    # Depending on what is decided above, the line is cleaned and copied - or ignored
                    if copy_fields:

                        # Decode line from bytes and strips off any new line characters
                        decoded_line = line

                        # New variable which is a regular expression used to pull ID & data from decoded_line.
                        # Simply put, it's making 2 groups. Group#1 is the string between 'id=' and '>', group#2 between
                        # '>' and '</f>'
                        entry = re.search("<f id=(.*)>(.*)</f>", decoded_line)

                        # These become key value pairs for 'story_dict'
                        key = entry.group(1)
                        value = entry.group(2)

                        if 'amp;' in value:
                            value = value.replace('amp;', '')

                        if 'gt;' in value:
                            value = value.replace('gt;', '')

                        if 'page' in key:  # change key from 'page-number' to 'page'
                            key = 'page'

                        if 'total' in key:  # change key from 'total-time' to 'total'
                            key = 'total'

                        # Else write key and value as they are
                        story_dict[key] = value

                    if copy_body:
                        # body = BeautifulSoup(line)
                        story_dict['body'] += BeautifulSoup(line, features="html.parser").text + '\n'

                if not break_out_flag:
                    # Append story_dict to 'data' list
                    self.data.append(story_dict)

                # # Close story file
                # story_file.close()

            # 8) Deletes the file we just read as it's no longer needed
            try:
                os.remove(local_dir + story_id_title)
            except PermissionError as e:
                print(str(datetime.datetime.now()), str(e))

    def set_backtimes(self):
        """
        Looping the the list of dicts, self.data, this process looks out for timing related key/values and gives each
        dict a new key, ['seconds'] with a calculated value of when the story goes on air. The observed key/values are:
        'back-time uec' = Infrequent hard-out, unchangeable timings for the show. e.g. start/end of show, opt
        'air-date' = If the show deviates from pre-set times, the PA manually sets this time. This takes precedent.
        'total-time uec' = The total-time/duration of the story

        note:~ iNews appends ' uec' to back-time and total-time keys IF they have values.

        Each loop looks for 'air-date' first, 'back-time' second, 'total-time' last.

        If air-date or back-time is found assign its value to the new key ['seconds'] and to the variable current_time
        If they also have 'total-time' then the variable next_time = current_time + total-time
        After this break out of the current loop iteration and begin the next

        If no air-date or back-time exists but a total-time does, then proceed to give dict['seconds'] the previously
        forecast next_time value.

        note:~ We have to work with current_time amd next_time vars in this way because just working with say
        ['seconds'] and total-time on the same story/dict would always result in calculating a seconds in the future

        Lastly, if no timing related value are found in the dict, dict['seconds'] = the previously set current_time
        """

        current_time = 0  # Used to add a value to the dict['seconds'] IF its duration/total-time == 00:00
        next_time = 0  # IF a duration/total-time IS detected, that value is added to current_time to forecast when the
        # current item will end and the next one begins

        for d in self.data:
            try:
                # 'air-date' comes in as epoch. converted to str(HH:MM:SS) sliced in to hrs, min, secs and converted
                # back to int to calculate seconds from midnight...
                air_hour = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[11:13])  # invalid int
                air_min = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[14:16])
                air_secs = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[17:19])

                # current_time, d['seconds'] = air-date time
                current_time = d['seconds'] = (air_hour * 3600) + (air_min * 60) + air_secs

                # safely add next_time if total available
                if 'total' in d:
                    next_time = current_time + int(d['total'])

                # convert seconds to readable back time
                d['backtime'] = str(datetime.timedelta(seconds=d['seconds']))
                continue
            except (KeyError, ValueError):  # Key: 'total-time uec' not in every dict, Val: air-date
                pass

            try:
                # current_time, d['seconds'] = back-time
                current_time = d['seconds'] = int(d['back-time uec'][1:])

                # so first line of if statement below has a value to input on its first use
                next_time = current_time

                # override next_time if actual total exists
                if 'total' in d:
                    next_time = current_time + int(d['total'])

                # convert seconds to readable back time
                d['backtime'] = str(datetime.timedelta(seconds=d['seconds']))
                continue
            except KeyError:  # try only works if 'back-time uec' present
                pass

            # if total present
            if 'total' in d:
                if not d['total']:
                    d['total'] = '0'
                d['seconds'] = next_time  # use the previously forecast time as new
                current_time = next_time  # ^
                next_time += int(d['total'])  # new forecast for next time change
            else:
                d['seconds'] = current_time  # if time values at all, send current_time

            d['backtime'] = str(datetime.timedelta(seconds=d['seconds']))  # and update readable backtime

            try:
                d.pop('back-time')
            except:
                pass

            # print(d['page'], d['title'], str(datetime.timedelta(seconds=d['seconds'])))

    def finishing_touches(self):
        # Rundowns are divided into sections, aka items, defined by the dict['page'] parameter. Scrollview utilises
        # these breaks to skip through each 'item'. The position of each item is set here. Some manipulation of pos
        # is needed to center pos in the middle of the screen - it drifts without this. We use 'Equation of a straight
        # line' (y=mx+b), to do so. See more here: https://www.mathsisfun.com/equation_of_line.html

        # intercepts
        b_pos = -0.0095
        b_neg = -0.0105

        # slope
        m = 0.001 / 0.05

        for index, story_dict in enumerate(reversed(self.data)):

            try:  # Catch occasional smaller dicts

                # Convert int total to MM:SS
                int_seconds = time.gmtime(int(story_dict['total']))
                story_dict['total'] = str(time.strftime("%M:%S", int_seconds))

                if story_dict['page'][-2:] == '00' or story_dict['page'][-1:] == '.':
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

            except KeyError as e:
                pass

        for r in range(12):
            self.data.append({
                "camera": "byebye",
                "format": "cya",
                "page": "9999",
                "story_id": "over",
                "seconds": 0,
                "backtime": '23:30:00',
                "total": "0",
                "focus": False,
                "title": " ~ end of show ~ "
            }, )

    def create_pv_version(self, export_path):
        slices = [0]

        for index, story_dict in enumerate(self.data):
            try:
                if 'tm' in export_path or 'lw' in export_path:
                    if story_dict['page'] and story_dict['page'][-1] == '.':
                        slices.append(index)
                elif 'lk' in export_path:
                    if story_dict['page'][-2:] == '00':
                        slices.append(index)
            except KeyError as e:
                pass

        # Using the current and next index from slice, attempt to store chunks
        # of self.data in new_dicts
        for a, b in zip(slices, slices[1:] + [slices[0]]):
            self.data_pv.append(self.data[a:b])

        if not self.data_pv[-1]:
            self.data_pv.pop()

    def create_json_files(self, export_path):
        """...Export..."""
        with open('exports/sv/' + export_path + '.json', 'w') as outfile:
            outfile.write(json.dumps(self.data, indent=4, sort_keys=True))

        with open('exports/pv/' + export_path + '.json', 'w') as outfile:
            outfile.write(json.dumps(self.data_pv, indent=4))
