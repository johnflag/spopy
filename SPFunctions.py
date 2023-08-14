# Import Libraries
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext, UserCredential
from office365.sharepoint.files.file import File
import os, csv, requests, re, time
from io import BytesIO, StringIO
import pandas as pd
from urllib.parse import urlparse


class SPFunctions:

    def __init__(self, sp_url: str, client_id: str, client_secret: str):
        """
        Initialize an instance of SPFunctions.

        Parameters:
            url (str): The SharePoint URL.
            client_id (str): The client ID for authentication.
            client_secret (str): The client secret for authentication.
        """

        """Create connection to Sharepoint Site"""

        self.sp_url = sp_url
        self.client_id = client_id
        self.client_secret = client_secret

        self.base_url = self.extract_sharepoint_base_url(self.sp_url)

        self.ctx = self.retry(self.sharepoint_authentication())

    def is_sharepoint_url(self, url: str):
        pattern = r"https:\/\/([\w\-]+)\.sharepoint\.com\/sites\/.*"

        if re.match(pattern, url) is None:
            raise Exception(f"{url} is not a valid SharePoint Site URL")

    def create_relative_url(self, sp_url: str):
        self.is_sharepoint_url(sp_url)

        file_name = sp_url.split("/")[-1]
        sp_path = "/sites" + sp_url.replace(file_name, "").split("sites")[-1]
        sp_path += file_name.split("?")[0]
        return sp_path

    def extract_sharepoint_base_url(self, sp_url: str):
        """
        Extract the base SharePoint site URL from a given SharePoint URL.

        Parameters:
            url (str): The SharePoint URL.

        Returns:
            str: The base SharePoint site URL.
        """
        self.is_sharepoint_url(sp_url)

        parsed_url = urlparse(sp_url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/sites/{parsed_url.path.split('/')[2]}"
        return base_url

    def extract_file_format(self, url):
        # Extract the file name from the URL
        file_name = os.path.basename(url)
        
        # Extract the file format (extension) from the file name
        file_format = os.path.splitext(file_name)[1][1:].lower()

        if file_format == '':
          raise Exception('Provided URL is not a file')
        
        return file_format

    def sharepoint_authentication(self):
        try:
            ctx_auth = AuthenticationContext(self.base_url)
            ctx_auth.acquire_token_for_app(self.client_id, self.client_secret)

            ctx = ClientContext(self.base_url, ctx_auth)

            web = ctx.web
            ctx.load(web)
            ctx.execute_query()

            print(f"Successfully authenticated to: {self.base_url}")

            return ctx

        except Exception as e:
            print("Authentication error!")
            print(e)

    def retry(self, function, max_retries: int = 10, retry_delay: int = 30):
        """
        Retry calling a function with a specified number of retries and backoff delay.

        This function attempts to call the provided function and retries a specified number of times
        in case of failure. It waits for a specified delay between retries.

        Parameters:
            function (callable): The function to be retried.

        Returns:
            callable: The result of the successful function call.

        Raises:
            Exception: If the maximum number of retries is reached without a successful call.
        """
        retry_count = 0
        last_exception = None

        while retry_count < max_retries:
            try:
                return function

            except Exception as e:
                last_exception = e
                logging.error(
                    f"Attempt {retry_count + 1}/{max_retries}: Connection failed. Retrying in {retry_delay} seconds..."
                )
                time.sleep(retry_delay)
                retry_count += 1

        print(f"Connection timed out after {max_retries} retries. Aborting.")
        raise last_exception

    def get_sp_files(self, sp_url: str, file_format: str = '', include_subfolders=False):
        """
        Get a list of SharePoint files with a specified file format.

        Parameters:
            file_format (str): The desired file format (default is 'csv').

        Returns:
            pd.DataFrame: A DataFrame containing file information (Name, URL, Last_Modified).
        """

        def _get_files_recursively(folder, file_format, recursive = False):
            files = folder.files
            self.ctx.load(files)
            self.ctx.execute_query()

            l = len(file_format)

            files_dic = []

            for file in files:
                if l > 0 and file_format == file.properties["Name"][-l:]:
                    files_dic.append(file.properties)
                if l == 0:
                    files_dic.append(file.properties)

            if recursive:

              # Recursively get files from subfolders
              folders = folder.folders
              self.ctx.load(folders)
              self.ctx.execute_query()

              for subfolder in folders:
                  subfolder_files = _get_files_recursively(subfolder, file_format, recursive=True)
                  files_dic.extend(subfolder_files)

            return files_dic

        folder_path = self.create_relative_url(sp_url)

        try:
            folder = self.ctx.web.get_folder_by_server_relative_url(folder_path)

            if include_subfolders:

              # Retrieve the folder using SharePoint client context
              self.ctx.load(folder)
              self.ctx.execute_query()

              # Recursively get files from the folder and its subfolders
              files_dic = _get_files_recursively(folder, file_format, recursive = True)

            else:

              files_dic = _get_files_recursively(folder, file_format, recursive = False)

            if len(files_dic) > 0:
                df_files = pd.DataFrame(files_dic)

                df_files["TimeLastModified"] = pd.to_datetime(
                    df_files["TimeLastModified"]
                ).dt.tz_convert(None)

                print("List of files was created")

                return df_files

            else:
                raise Exception("No files found")

        except Exception as e:
            print(e)

    def get_local_files(self, local_location: str = ".", file_format: str = ''):
        """
        Get metadata of files stored in a local directory.

        Parameters:
            local_location (str): The local directory to search for files (default is current directory).

        Returns:
            pd.DataFrame: A DataFrame containing file metadata (Name, Path, Size, Last_Modified).
        """

        local_location = (
            (local_location + "/") if local_location[-1] != "/" else local_location
        )

        file_list = []
        l = len(file_format)

        df = pd.DataFrame(columns=["Name", "Path", "Size", "TimeLastModified"])

        for filename in os.listdir(local_location):
            file_path = os.path.join(local_location, filename)
            if os.path.isfile(file_path):
                file_info = {
                    "Name": filename,
                    "Path": file_path,
                    "Size": os.path.getsize(file_path),  # in bytes
                    "TimeLastModified": os.path.getmtime(file_path),  # timestamp
                }

                if l > 0 and file_format == file_info['Name'][-l:]:
                    df = df.append(file_info, ignore_index=True)
                if l == 0:
                    df = df.append(file_info, ignore_index=True)

        if len(df) > 0:
            df["TimeLastModified"] = pd.to_datetime(df["TimeLastModified"], unit="s")

        return df

    def download_file(self, relative_url: str, local_location: str = '.'):
        response = File.open_binary(
            self.ctx, relative_url
        )  # save file from sharepoint as binary
        if str(response.status_code) == "200":
            with open(local_location, "wb") as local_file:
                local_file.write(response.content)  # write in your pc
        else:
            print(f"Return with error code : {response.status_code}")
            print(f"Content of error : {response.content}")
            raise Exception("Cannot Download File")

    def download_all_files(
        self, 
        sp_url: str, 
        local_location: str='.',
        file_format: str = '',
        include_subfolders: bool = False
      ):

        local_location = (
            (local_location + "/") if local_location[-1] != "/" else local_location
        )

        try:
            os.makedirs(local_location)
            print(f"Directory created at '{local_location}'.")
        except FileExistsError:
            # Directory already exists, do nothing
            pass

        df_files = self.retry(
            self.get_sp_files(sp_url, file_format, include_subfolders)
        )

        for index, row in df_files.iterrows():
            try:
                self.retry(
                    self.download_file(
                        sp_url=row['ServerRelativeUrl'],
                        local_location=os.path.join(local_location, row["Name"]),
                    )
                )
                print(row["Name"] + " downloaded!")
            except Exception as e:
                print(f"Failed to download {row['Name']}")
                raise(e)

        print("All Files Downloaded")

    def download_new_files(
        self, 
        sp_url: str, 
        local_location: str = '.', 
        file_format: str = '', 
        include_subfolders: bool = False, 
        keep_deleted: bool = False
      ):
        """
        Download new SharePoint files to a local directory and optionally remove deleted files.

        Args:
            sp_url (str): The URL of the SharePoint site.
            local_location (str, optional): The local directory to save the downloaded files (default is current directory).
            file_format (str, optional): The desired file format (default is '').
            include_subfolders (bool, optional): Whether to include subfolders when searching for files (default is False).
            keep_deleted (bool, optional): Whether to keep deleted SharePoint files in local directory (default is False).
        """

        local_location = (
            (local_location + "/") if local_location[-1] != "/" else local_location
        )

        df_current_files = self.get_local_files(local_location)

        df_sp_files = self.retry(
            self.get_sp_files(sp_url, file_format)
        )

        df_new = df_sp_files.merge(df_current_files, on="Name", how="left")
        df_new = df_new.loc[
            (df_new["TimeLastModified_x"] > df_new["TimeLastModified_y"])
            | (df_new["TimeLastModified_y"].isna())
        ]

        if len(df_new) > 0:
            for index, row in df_new.iterrows():
                try:
                    self.retry(
                        self.download_file(
                            sp_url=row['ServerRelativeUrl'],
                            local_location=os.path.join(local_location, row["Name"]),
                        )
                    )

                    print(row["Name"] + " downloaded!")

                except Exception as e:
                    print(f"Failed to download {row['Name']}")
                    raise(e)

            print("All New Files Downloaded")

        else:
            print("No new files found!")

        if not keep_deleted:
            df_deleted = df_current_files.merge(df_sp_files, on="Name", how="left")
            df_deleted = df_deleted.loc[df_deleted["TimeLastModified_y"].isna()]

        if len(df_deleted) > 0:
            for index, row in df_deleted.iterrows():
                try:
                    os.remove(row["Path"])
                    print(row["Name"] + " removed!")

                except Exception as e:
                    print(f"Failed to remove {row['Name']}: {e}")

    def read_file(self, 
                 sp_url: str,
                 sheet_name: str = 'Sheet1', 
                 as_pandas = False,
                 **kwargs
                ):
      
        """Download file from sharepoint or Read Excel/csv from sharepoint as pd dataframe"""
        file_format = self.extract_file_format(sp_url)
        relative_url = self.create_relative_url(sp_url)

        response = File.open_binary(
            self.ctx, relative_url
        )  # save file from sharepoint as binary

        if str(response.status_code) == '200' :
            toread = BytesIO()
            toread.write(response.content)  # pass your `decrypted` string as the argument here
            toread.seek(0)  # reset the pointer

            if as_pandas:
              if file_format in ['csv', 'txt']:
                  return pd.read_csv(toread, **kwargs)
              elif file_format in ['xlsx','xls']:
                  return pd.read_excel(toread, sheet_name = sheet_name, **kwargs)
              else:
                 raise Exception('Format currently not supported')                 
            else :
               return toread
        else :
            print(f'Return with error code : {response.status_code}')
            print(f'Content of error : {response.content}')
            raise Exception('Cannot Download File')

    def read_multiple_files(self, 
                 sp_url: str,
                 file_format: str,
                 include_subfolders: bool = False,
                 sheet_name: str = 'Sheet1', 
                 as_pandas = False,
                 **kwargs
                ):

      if file_format == '':
        raise Exception('File format must be specified.')

      df_files = self.get_sp_files(sp_url, file_format, include_subfolders)

      files = []

      for index, row in df_files.iterrows():

          response = File.open_binary(
              self.ctx, row['ServerRelativeUrl']
          )  # save file from sharepoint as binary

          if str(response.status_code) == '200' :
              toread = BytesIO()
              toread.write(response.content)  # pass your `decrypted` string as the argument here
              toread.seek(0)  # reset the pointer

              if as_pandas:
                if file_format in ['csv', 'txt']:
                    files.append(pd.read_csv(toread, **kwargs))
                elif file_format in ['xlsx','xls']:
                    files.append(pd.read_excel(toread, sheet_name = sheet_name, **kwargs))
                else:
                    raise Exception('Format currently not supported')              
              else :
                  files.append(toread)
                
          else :
              print(f'Return with error code : {response.status_code}')
              print(f'Content of error : {response.content}')
              raise Exception('Cannot Download File')
      
      if as_pandas:
          files = pd.concat(files)

      return files