#Testing 
#Final Script for Taking Feather counts for required attributes.(Has Value)
import os
import glob
import json
import pandas as pd
import datetime

# Specify the full path to your JSON file
json_file_path='C:/Users/P Santhoshkumar/Desktop/Excel_CattypeAttribute/Hdfc_For_Fetching_Counts.json'

# Load the JSON data from the file
with open(json_file_path) as json_file:
    data = json.load(json_file)

Final_result = pd.DataFrame()
#final_dataframe=pd.DataFrame()
for k in data:
  
    if (data[k]['Cattype']) =='CIHMaster':
      
        st=glob.glob(f"D:\\Python Projects\\Parent\\{data[k]['Cattype']}")
      
    else:
        st=glob.glob(f"D:\\Python Projects\\Child\\{data[k]['Cattype']}")

    dfList = list()

    for path in st:
        df = pd.read_feather(path, columns=[f"{data[k]['Attributes']}", 'R1_s'])
        dfList.append(df)
        #print(dfList) 
    final = pd.concat(dfList)
   # print(final)
    # final['LatestSalCredit_d'].value_counts()
    if  data[k]['Conditions'] == 'Has Value':
        final = final[final[f"{data[k]['Attributes']}"]!=None ]
        final = final[final[f"{data[k]['Attributes']}"].isin(final[f"{data[k]['Attributes']}"].dropna().tolist())]
        attribute_shape=final['R1_s'].nunique()
        
       # print(attribute_shape)
        #print(len(attribute_counts))

    elif data[k]['Conditions'] =='EqualTo': 
        case1 = final[final[f"{data[k]['Attributes']}"].isin([data[k]['Value']])]
        attribute_shape=len(case1)
        

    elif data[k]['Conditions'] =='Contains': 
        value_for_Y = data[k]['Value'].values()
        case1 = final[final[f"{data[k]['Attributes']}"].isin(value_for_Y)]
        attribute_shape=len(case1)


    elif (data[k]['Conditions'] =='GreaterThanorEqualTo' or  data[k]['Conditions'] =='LessThanOrEqualTo'):
          
          op = '>=' if '>=' in data[k]['Value'] else '<='

          if op =='<=':

            num1 =  (data[k]['Value'].replace('>=','').replace('<=','').replace("'",""))
          #num=pd.to_datetime(num2).dt.date
            arg3=num1
            arg3 = datetime.datetime.strptime(arg3, "%d-%m-%Y").date()
            final[f"{data[k]['Attributes']}"] = pd.to_datetime(final[f"{data[k]['Attributes']}"], errors='coerce').dt.date
            resul= final[(final[f"{data[k]['Attributes']}"])<=arg3]
            attribute_shape=len(resul)

          else:
            num2 =  (data[k]['Value'].replace('>=','').replace('<=','').replace("'",""))
          #num=pd.to_datetime(num2).dt.date
            arg3=num2
            arg3 = datetime.datetime.strptime(arg3, '%d-%m-%Y').date()
            final[f"{data[k]['Attributes']}"] = pd.to_datetime(final[f"{data[k]['Attributes']}"], errors='coerce').dt.date
            resul= final[(final[f"{data[k]['Attributes']}"])>=arg3]
            attribute_shape=len(resul)
    

    elif (data[k]['Conditions'] =='GreaterThan' or  data[k]['Conditions'] =='LessThan') and  '_dt' not in data[k]['Attributes'] :
        
        num =  int(data[k]['Value'].replace('>','').replace('<','').replace(' ',''))
        op = '>' if '>' in data[k]['Value'] else '<'
        if op == '>':
            case1 = final[final[f"{data[k]['Attributes']}"] > num]
            attribute_shape=len(case1)
        else:
            case1 = final[final[f"{data[k]['Attributes']}"] < num]
            attribute_shape=len(case1)


    elif (data[k]['Conditions'] =='GreaterThan' or  data[k]['Conditions'] =='LessThan') and  '_dt' in data[k]['Attributes']:
       
        op1 = '>' if '>' in data[k]['Value'] else '<'

        if op1=='<':
            num2 =  (data[k]['Value'].replace('>','').replace('<','').replace("'",""))
          #num=pd.to_datetime(num2).dt.date
            arg4=num2
            arg4 = datetime.datetime.strptime(arg4, "%d-%m-%Y").date()
            final[f"{data[k]['Attributes']}"] = pd.to_datetime(final[f"{data[k]['Attributes']}"], errors='coerce').dt.date
            resul1= final[(final[f"{data[k]['Attributes']}"])<arg4]
            attribute_shape=len(resul1)
        else:
            num2 =  (data[k]['Value'].replace('>','').replace('<','').replace("'",""))
          #num=pd.to_datetime(num2).dt.date
            arg4=num2
            arg4 = datetime.datetime.strptime(arg4, "%d-%m-%Y").date()
            final[f"{data[k]['Attributes']}"] = pd.to_datetime(final[f"{data[k]['Attributes']}"], errors='coerce').dt.date
            resul1= final[(final[f"{data[k]['Attributes']}"])<arg4]
            attribute_shape=len(resul1)


    data_= {'Cattype':(data[k]['Cattype']),
            'Attributes':(data[k]['Attributes']) ,
            'Filter':(data[k]['Conditions']),
            'Value':(data[k]['Value']),
            'count':(attribute_shape)}
    result = pd.DataFrame(data_,index=[0])
         # print(result)
         # result.to_csv('Final_output.csv',index=False)
         # final_dataframe = final_dataframe.append(result, ignore_index=True)
         # print(final_dataframe)
    Final_result = pd.concat([Final_result, result], ignore_index=True)
    print(Final_result)
    TimeStamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    path1=f'D:\Python Projects\Final_{TimeStamp}.csv';
Final_result.to_csv(path1, index=True)

