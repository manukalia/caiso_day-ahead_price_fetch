# CAISO Electricity Day-ahead Price Fetch Utility
# Manu Kalia
# 05-Dec-2019
# All rights reserved



import pandas as pd
import wget, os, zipfile, glob, shutil
import time, pickle, re
import PySimpleGUI as sg



#    FUNCTION DEFINITIONS


# Function to generate a datetime range ...
#   parameters:  datetime range start (str), end (str),
#                and frequency (defaults to 'H')

def gen_daterange(query_start, query_end, query_freq='H'):
    return pd.date_range(start = query_start,
                         end   = query_end,
                         freq  = query_freq)


# Function to determine number of batches and the size of the last batch
#   parameters:  date index, batch size (no. elements per batch), k (for-loop index)

def create_batches(date_index, batch_size):
    if len(date_index) % batch_size == 0:
        num_batches = len(date_index) // batch_size
        final_batch_size = batch_size
    else:
        num_batches = len(date_index) // batch_size + 1
        final_batch_size = len(date_index) % batch_size

    return num_batches, final_batch_size



# Function to determine number of batches and the size of the last batch
#   parameters:  date index, batch size (no. elements per batch), final batch size,
#                k (for-loop index)

def gen_batch_start_end(date_index, batch_size, final_batch_size, k):
    start_datetime = date_index[k * batch_size]

    if i == num_batches - 1:
        end_datetime   = date_index[(k * batch_size) + final_batch_size - 1]
    else:
        end_datetime   = date_index[(k+1) * batch_size - 1]

    start_arg = f'{start_datetime.year}'\
+f'{start_datetime.month}'.zfill(2)\
+f'{start_datetime.day}'.zfill(2)+'T'\
+f'{start_datetime.hour}'.zfill(2)+':00-0000'

    end_arg = f'{end_datetime.year}'\
+f'{end_datetime.month}'.zfill(2)\
+f'{end_datetime.day}'.zfill(2)+'T'\
+f'{end_datetime.hour}'.zfill(2)+':00-0000'

    return start_arg, end_arg


# Function to construct a GENERIC one-month .csv LMP price download
#    data query for the CAISO OASIS API

# Parameters (all strings):
#    node                       name of node
#

def gen_price_query(queryname, version, start_arg, end_arg, market_run_id, node):
    oasis_website = 'oasis.caiso.com'
    context_path  = 'oasisapi'
    url = f'http://{oasis_website}/{context_path}/SingleZip'
    resultformat  =  '6'

    query = f'{url}?resultformat={resultformat}&queryname={queryname}&version={version}\
&startdatetime={start_arg}&enddatetime={end_arg}\
&market_run_id={market_run_id}&node={node}'

    return query


# Function unzips all files in a specified targetdirectory,
# saving unzipped files to a specified destination directory
# Parameters:  download (target dir), unzipped destination dir

def unzip_dir(download_dir, unzipped_dest_dir):
    for item in os.listdir(download_dir):           # loop through items in dir
        if item.split('.')[-1] == 'zip':            # check for zip extension
            file_name = f'{download_dir}{item}'     # get relative path of files
            print(f'unzipping... {file_name}')
            zip_ref = zipfile.ZipFile(file_name)    # create zipfile object
            with zip_ref as destination:
                destination.extractall(unzipped_dest_dir)
        else: continue




#  USER INPUTS WINDOW LOOP


sg.ChangeLookAndFeel('GreenMono')

input_window_layout = [
    [sg.Text('\nEnter a CAISO nodename  (or comma-separated list of\n\
nodenames) below.  Then hit the SUBMIT button to proceed ...',
        font=('Raleway', 18))],
    [sg.InputText('BAYSHOR2_1_N001, LCIENEGA_6_N001, SOUTHBY_6_N001',
        key='nodename_string',
        font=("Raleway", 14),
        size=(55, 1))],

    [sg.Text('\n\n', font=("Raleway", 6))],
    [sg.Text('Enter the start & end datetimes for the hourly price downloads:',
        font=("Raleway", 18))],
    [sg.InputText('2019-01-01 00:00:00',
        key='start_date',
        font=("Raleway", 14),
        size=(20, 1)), sg.CalendarButton('select start date', target='start_date')],
    [sg.InputText('2019-12-01 00:00:00',
        key='end_date',
        font=("Raleway", 14),
        size=(20, 1)), sg.CalendarButton('select end  date', target='end_date')],

    [sg.Text('\n\n', font=("Raleway", 6))],
    [sg.Frame(layout=[
        [sg.Checkbox('Keep downloaded .zip files',
            size=(25,1),
            font=("Raleway", 14),
            default=False,
            key='keep_zipped_files'),
        sg.Checkbox('Keep unzipped .csv files',
            size=(25,1),
            font=("Raleway", 14),
            key='keep_unzipped_files',
            default=False)],
        [sg.Checkbox('Save output as .csv',
            size=(25,1),
            font=("Raleway", 14),
            key='save_csv_files',
            default=True)],
        [sg.Checkbox('Save output as binarized Pandas dataframe (.pkl)',
            size=(40,1),
            font=("Raleway", 14),
            key='save_dataframe',
            default=True)]
            ],
        title='FILE SAVE OPTIONS',
        font=("Raleway", 18),
        title_color='darkblue',
        relief=sg.RELIEF_SUNKEN)
        ],

    [sg.Text('\n', font=("Raleway", 6))],
    [sg.Text('_' * 80)],
    [sg.Text('Choose filesave location:', font=("Raleway", 14))],
    [sg.InputText('/Users/owner',
        key='destination_folder',
        size=(55, 1),
        font=("Raleway", 14)),
        sg.FolderBrowse(target='destination_folder')],
    [sg.Submit(), sg.Cancel()]]

input_window = sg.Window('CAISO Day-ahead Prices Fetch Utility',
    input_window_layout,
    location=(180, 120),
    default_element_size=(20, 1),
    grab_anywhere=False)

event, user_input_dict = input_window.read()
input_window.close()


sg.PopupScrolled('\n\nwindow auto closes in 15 sec   (or hit OK to close)\n\n',
                 'You entered the following parameters:\n',
                 f'Node names:  {user_input_dict["nodename_string"]}',
                 f'Start date:     {user_input_dict["start_date"]}',
                 f'End date:      {user_input_dict["end_date"]}',
                 f'Keep zipped files:     {user_input_dict["keep_zipped_files"]}',
                 f'Keep unzipped files:    {user_input_dict["keep_unzipped_files"]}',
                 f'Save output .csv files:  {user_input_dict["save_csv_files"]}',
                 f'Save output dataframes:  {user_input_dict["save_dataframe"]}',
                 f'Destination folder:  {user_input_dict["destination_folder"]}',
                 font=('Raleway', 18),
                 title='',
                 size=(80, 20),
                 location=(180, 120),
                 auto_close=True,
                 auto_close_duration=15)


with open(f"{user_input_dict['destination_folder']}/user_input_dict.pkl", 'wb') as f:
    pickle.dump(user_input_dict, f)


# Parameters: API-query-starting datetime, ending datetime, and frequency,
#               batch size (no. of elements per batch), node (id str), delay (sec))

nodename_list = re.split('\W+', user_input_dict['nodename_string'])

query_start = user_input_dict['start_date']
query_end   = user_input_dict['end_date']

batch_size  = 24*28               # 28 days is safely less than 31 day API restriction
delay = 5                         # No. seconds to delay between API queries


# Call the appropriate functions and store returned results into variable names

date_index = gen_daterange(query_start, query_end)
num_batches, final_batch_size = create_batches(date_index, batch_size)


# Create temporary working directories for downloads and unzipped files

pathname = user_input_dict['destination_folder'] + '/temp_dir/'
download_pathname = pathname + 'downloads/'
unzipped_pathname = pathname + 'unzipped/'

os.mkdir(pathname)
os.mkdir(download_pathname)
os.mkdir(unzipped_pathname)



# Progress Bar and Data Fetch Loop

num_iterations = num_batches * len(nodename_list)

# Progress Bar Layout

prog_bar_layout = [[sg.Text('CAISO DA Price Download Progress',
                            font=('Raleway', 18))],
                   [sg.ProgressBar(num_iterations,
                                   orientation='h',
                                   size=(60, 12),
                                   key='progbar')],
                   [sg.Cancel()]]


# Create Progress Bar Window

prog_bar_window = sg.Window('CAISO DA Prices Download Progress',
    prog_bar_layout,
    location=(180, 120))

counter = 0

for node in nodename_list:
    for i in range(num_batches):
        event, values = prog_bar_window.read(timeout=100)
        if event == 'Cancel' or event is None: break
        start_arg, end_arg = gen_batch_start_end(date_index,
                                                 batch_size,
                                                 final_batch_size,
                                                 i)
        wget.download(gen_price_query('PRC_LMP',
                                  '1',
                                  start_arg,
                                  end_arg,
                                  'DAM',
                                  node),
                      download_pathname)
        counter += 1
        prog_bar_window['progbar'].update_bar(counter)
        time.sleep(delay)

time.sleep(4)             # wait 4 sec before closing prog bar window
prog_bar_window.close()

unzip_dir(download_pathname, unzipped_pathname)



# Create single combined dataframe of all downloaded data

dam_orig_cols=['INTERVALSTARTTIME_GMT',
               'NODE',
               'MARKET_RUN_ID',
               'XML_DATA_ITEM',
               'MW']

dam_new_cols =['datetime',
               'node',
               'market',
               'price_component',
               'dam_price_per_mwh']

dam_rename_dict = {old: new for old, new in zip(dam_orig_cols, dam_new_cols)}

dam_df = pd.DataFrame(columns=dam_new_cols)

for file in glob.glob(unzipped_pathname +'*.csv'):
    df = pd.read_csv(file, usecols=dam_orig_cols).rename(index=str,
                                                         columns=dam_rename_dict)
    df = df[df.price_component == 'LMP_PRC']
    dam_df = dam_df.append(df, ignore_index=True)

dam_df = dam_df.sort_values(by='datetime').reset_index(drop=True)

dam_df['datetime'] = pd.to_datetime(dam_df['datetime'], utc=True)
dam_df.set_index('datetime', inplace=True)
dam_df.sort_index(inplace=True)

dam_df.index = dam_df.index.tz_convert('US/Pacific')     # not sure if this is the right adjustment



# Split Combined Datatrame into Separate Node-specific Dataframes

new_nodenames = [node[:node.find('_')] if node.find('_') != (-1) else node for node in nodename_list]

df_list = []
df_names_list = []
da_price_col_names = []

for index, node in enumerate(nodename_list):
    df_list.append(new_nodenames[index] + '_df')
    df_names_list.append(new_nodenames[index] + '_df')
    da_price_col_names.append(new_nodenames[index] + '_da_price_per_mwh')

for i in range(len(new_nodenames)):
    temp_dict = dam_df[dam_df.node == nodename_list[i]].to_dict('index')
    df_list[i] = pd.DataFrame.from_dict(temp_dict, orient='index')
    df_list[i].rename(columns={'dam_price_per_mwh': da_price_col_names[i]},
                      inplace=True)
    df_list[i].drop(columns=['node'], inplace=True)



# Recombine Nodal DF's into a Single Datafram with Side-by-side Price Columns

if len(df_names_list) <= 1: pass

elif len(df_names_list) == 2:
    CAISO_DA_PRICES_mult_cols_df = df_list[0].join(df_list[1][da_price_col_names[1]],
                                          how='outer')
    df_names_list.append('CAISO_DA_PRICES_mult_cols_df')
    df_list.append(CAISO_DA_PRICES_mult_cols_df)

else:
    CAISO_DA_PRICES_mult_cols_df = df_list[0].join(df_list[1][da_price_col_names[1]],
                                          how='outer')
    for i in range(2, len(df_list)):
        CAISO_DA_PRICES_mult_cols_df = CAISO_DA_PRICES_mult_cols_df.join(df_list[i][da_price_col_names[i]],
                                                       how='outer')
    df_names_list.append('CAISO_DA_PRICES_mult_cols_df')
    df_list.append(CAISO_DA_PRICES_mult_cols_df)



# HOUSEKEEPING ...
#    -  Save dataframes as .pkl binary files
#    -  Save dataframes to .csv files
#    -  If specified by user, delete temp directory, downloaded .zip files, & unzipped .csv files

orig_path = pathname.replace('temp_dir/', '')

for index, df in enumerate(df_list):
    if user_input_dict['save_dataframe']:
        with open(orig_path + df_names_list[index] + '.pkl', 'wb') as f:
            pickle.dump(df, f)

    if user_input_dict['save_csv_files']:
        df.to_csv(orig_path + df_names_list[index] + '.csv')

if (user_input_dict['keep_zipped_files'] == False) and (user_input_dict['keep_unzipped_files'] == False):
    shutil.rmtree(pathname, ignore_errors=True)

elif user_input_dict['keep_zipped_files'] == False:
    shutil.rmtree(download_pathname, ignore_errors=True)

elif user_input_dict['keep_unzipped_files'] == False:
    shutil.rmtree(unzipped_pathname, ignore_errors=True)



# Program Complete Message

sg.PopupScrolled('\n\nwindow auto closes in 15 sec   (or hit OK to close)\n\n',
                 'Program is Complete!  Check your destination directory for saved files:\n',
                 f'Destination folder:  {user_input_dict["destination_folder"]}',
                 font=('Raleway', 18),
                 title='',
                 size=(40, 10),
                 location=(180, 120),
                 auto_close=True,
                 auto_close_duration=10)
