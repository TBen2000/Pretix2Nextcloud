# Customized Pretix2Nextcloud instance for kv-stuttgart-jungschartag
# Needs P2N.py to function

import pandas as pd
import logging
import sys
import os

# Add current directory
sys.path.append(os.path.dirname(__file__))
# Add parent directory
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# import P2N.py
try:
    from P2N import Main, Environment, PretixAPI
except ImportError:
    raise ImportError("Could not import P2N.py from current or parent directory.")


Environment().set_defaults(
    default_pretix_url="https://tickets.swdec.de",
    default_pretix_organizer_slug="kv-stuttgart",
    default_excel_max_column_width=30,
    default_temp_dir_name="p2n_teencamp",
    default_nextcloud_url="https://jcloud.swdec.de",
    default_nextcloud_upload_dir="Anmeldungen_Teencamp",
    default_timezone="Europe/Berlin",
    default_interval_minutes=15,
    default_check_interval_seconds=60,
    default_run_once=False,
    default_logging_level="INFO",
)


class Dataframe:
    last_raw_df = pd.DataFrame()  # global variable to store last fetched raw dataframe

    def __init__(self, success_on_last_run: bool = False):
        """
        Initialize, fetch data from Pretix API and load into different desired dataframes.
        """
        pretix = PretixAPI()
        env = Environment()

        self.time_zone = env.get_timezone()

        self.raw_df = pretix.get_raw_df()

        if success_on_last_run and self.__class__.last_raw_df.equals(self.raw_df):
            raise Exception("No changes in data since last fetch.")
        self.__class__.last_raw_df = self.raw_df
        
        self.towns_list = pretix.get_question_choices_by_text("Ich melde mich über folgende Ortschaft an")

        self.sorted_df = self._get_sorted_df()
        self.town_dfs = self._get_town_dfs()
        self.numbers_overview = self._get_numbers_df()


    def _get_sorted_df(self) -> pd.DataFrame:
        """
        Process raw dataframe to create a sorted dataframe with required columns.
        """

        df = self.raw_df

        # removed all cancelled registrations
        df = df[df["status"] != "c"]

        # rename needed columns
        renames = {
            "order_code": "Bestellnummer",
            "email": "E-Mail",
            "date": "Anmeldedatum",
            "item_name": "Art",
            "attendee_firstname": "Vorname",
            "attendee_lastname": "Nachname",
            "Essensunverträglichkeiten": "Essensunverträglichkeiten Ja/Nein",
            "Welche Unverträglichkeiten?": "Essensunverträglichkeiten",
            "Ich melde mich über folgende Ortschaft an": "Ortschaft",
            "Ich biete eine Fahrgemeinschaft an": "Fahrer Angebot",
            "Ich bin Ortsverantwortlicher.": "Ortsverantwortlicher",
            "Telefonnummer": "Telefonnummer Mitarbeiter",
        }
        df = df.rename(columns=renames)

        # combine "Telefonnummer der Eltern" and "Telefonnummer Mitarbeiter" to one column "Telefonnummer"
        df["Telefonnummer"] = df["Telefonnummer der Eltern"].combine_first(
            df["Telefonnummer Mitarbeiter"]
        )

        # filter for columns and set their order
        wanted_columns = [
            "Ortschaft",
            "Art",
            "Nachname",
            "Vorname",
            "Telefonnummer",
            "E-Mail",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Sonstiges",
            "Ortsverantwortlicher",
            "Fahrer Angebot",
            "Anmeldedatum",
            "Bestellnummer",
        ]
        df = df.filter(wanted_columns)

        # change date format
        df["Anmeldedatum"] = (
            pd.to_datetime(df["Anmeldedatum"], utc=True)
            .dt.tz_convert(self.time_zone)
            .dt.strftime("%Y-%m-%d %H:%M")
        )

        # sort (by "Ortschaft", then by "Art", then by "Nachname" and then by "Vorname") and reset index numbers
        df = df.sort_values(
            by=["Ortschaft", "Art", "Nachname", "Vorname"], ascending=True
        )
        df.index = range(1, len(df) + 1)

        logging.info("Sorted raw data.")

        return df

    def _get_town_dfs(self) -> dict[str, pd.DataFrame]:
        """
        Process sorted dataframe to create a dictionary of dataframes filtered by town.
        """

        df = self.sorted_df

        # filter for columns and set their order
        wanted_columns = [
            "Ortschaft",
            "Art",
            "Nachname",
            "Vorname",
            "Telefonnummer",
            "E-Mail",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Sonstiges",
            "Fahrer Angebot",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)

        # sort by town:
        df_by_town_dict = {}
        for town in self.towns_list:
            # filter by town, drop column "Ortschaft" and reset index numbers
            town_df = df[df["Ortschaft"] == town]
            town_df = town_df.drop(columns=["Ortschaft"])
            town_df.index = range(1, len(town_df) + 1)

            df_by_town_dict[town] = town_df

        logging.info("Filtered sorted data by town.")

        return df_by_town_dict

    def _get_numbers_df(self) -> pd.DataFrame:
        """
        Calculate and return a dataframe with counts of "Jungscharler", "Mitarbeiter", and total by town.
        """

        numbers_df = pd.DataFrame(
            {"Ortschaft": [], "Jungscharler": [], "Mitarbeiter": [], "Gesamt": []}
        )

        # make Ortschaft the index
        numbers_df.set_index("Ortschaft", inplace=True)

        df = self.sorted_df

        number_of_kids = len(df[df["Art"].str.contains("Jungscharler", na=False)])
        number_of_staff = len(df[df["Art"].str.contains("Mitarbeiter", na=False)])
        number_total = number_of_kids + number_of_staff

        # add row to numbers_df
        numbers_df.loc["GESAMT"] = [number_of_kids, number_of_staff, number_total]

        # filter by town:
        for town in self.towns_list:
            town_df = df[df["Ortschaft"] == town]

            town_kids = len(
                town_df[town_df["Art"].str.contains("Jungscharler", na=False)]
            )
            town_staff = len(
                town_df[town_df["Art"].str.contains("Mitarbeiter", na=False)]
            )
            town_total = town_kids + town_staff

            # add row to numbers_df
            numbers_df.loc[town] = [town_kids, town_staff, town_total]

        return numbers_df


class CustomMain(Main):
    def main(self):
        """
        Main function to generate Excel files and upload them to Nextcloud.
        """
        
        # fetch and sort data
        dataframe = Dataframe(self.success_on_last_run)

        # generate and upload excel file for raw data
        self.upload(dataframe.raw_df, "Raw_Data", add_filters=True)

        # generate and upload excel file for all attendees
        self.upload(dataframe.sorted_df, "Alle.xlsx", add_filters=True)

        # generate and upload excel file for town-wise attendees
        for town in dataframe.town_dfs:
            df = dataframe.town_dfs[town]
            self.upload(df, town, subdir="Nach_Orten")

        # generate and upload excel file for numbers overview
        self.upload(dataframe.numbers_overview, "Anmeldezahlen")

        self.nc.upload_last_updated()
        
        self.nc.upload_docker_image_version()


if __name__ == "__main__":
    CustomMain().run()
