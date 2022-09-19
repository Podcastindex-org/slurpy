# slurpy
A podcast enclosure downloader tuned for the Podcast Index weekly database dump.

## Worklog

v0.1.4
- Handle the request not sending properly because of, perhaps, domain not existing or other issues.
- Added a "start_at_id" commandline argument. Defaults to 1 if not present.

v0.1.3
- Better cooldown handling and some more refactoring to skip through existing files better.