# covid-act-bot


This is a basic Telegram bot that scrapes information from the ACT Government Covid 19 Exposures page at https://www.covid19.act.gov.au/act-status-and-response/act-covid-19-exposure-locations and posts them in Telegram as they are updated. It does this by scraping the tables, then looking at the contents of the table rows for differences. All this is stored in Redis as a back end.

If you just want to use it, https://t.me/ACTCovidExposureBot. You will need to issue the `/start` command in Telegram to enable updates.