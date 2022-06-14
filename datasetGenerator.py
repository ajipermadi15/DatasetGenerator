from operator import index
from mysqlx import Column
import pandas as pd
import random
import numpy as np
import string
import time
from datetime import datetime, date

path_dictionary = {'PATH_POSTAL_CODE': 'data/first_row_header_db_postal_code_data.csv',
                   'PATH_PROVINCES_CODE': 'data/first_row_header_db_province_data.csv',
                   'PATH_NAME': 'data/name_dataframe.csv',
                   'PATH_EMAIL_DOMAIN': 'data/all_email_provider_domains.txt'}

class DatasetGenerator():
    def __init__(self, seed=None, path_dictionary=path_dictionary):
        self.random = random
        if seed != None:
            self.random.seed(seed)
        self.name_dataframe = pd.read_csv(path_dictionary['PATH_NAME'])
        #self.name_dataframe.set_index("#", inplace=True)
        self.name_dataframe.drop('#', axis=1, inplace=True)
        self.postal_code_dataframe = pd.read_csv(path_dictionary['PATH_POSTAL_CODE'])
        #self.postal_code_dataframe.set_index('id', inplace=True)
        self.postal_code_dataframe.drop('id', axis=1, inplace=True)
        self.provinces_code_dataframe = pd.read_csv(path_dictionary['PATH_PROVINCES_CODE'])

        file_email_domain = open(path_dictionary["PATH_EMAIL_DOMAIN"], "r")
        read_email_domain_txt = file_email_domain.read()
        self.email_domain = read_email_domain_txt.split('\n')

    def lastName(self, dataframe):
        last_name_series = pd.Series([np.NaN for i in range(self.rows)])
        last_name_series.index = dataframe.index

        sex_value_counts = dataframe['sex'].value_counts()
        for idx in sex_value_counts.index:
            sex_filter_name = self.name_dataframe[self.name_dataframe["sex"] == idx]['name'].values
            dataframe_idx = dataframe[dataframe['sex'] == idx].index
            last_name_series[dataframe_idx] = self.random.choices(sex_filter_name, k=sex_value_counts[idx])
        ##print(len(last_name_series))
        return last_name_series
    
    def generateDummyName(self):
        population = list(self.name_dataframe.index)
        index_list = self.random.sample(population, k=self.rows)
        ##print(pd.Series(index_list).value_counts())
        ##print(len(index_list))
        dummy_name_dataframe = self.name_dataframe.loc[index_list]
        #dummy_name_dataframe.to_csv('error.csv')
        ##print(len(dummy_name_dataframe))
        ##print('generateDummyName - sukses 1')

        if self.last_name == True:
            last_name_series = self.lastName(dummy_name_dataframe)
            dummy_name_dataframe['last_name'] = last_name_series
            dummy_name_dataframe.rename(columns = {'name':'first_name'}, inplace=True)
            dummy_name_dataframe = dummy_name_dataframe[['first_name', 'last_name', 'sex', 'nation']]
 
        old_sex = ['Masculine', 'Feminine', 'Unisex']
        new_sex = ['M', 'F', 'Unknown']
        dummy_name_dataframe['sex'].replace(old_sex, new_sex, inplace=True)
        dummy_name_dataframe.index = self.index
        #print("len dummy name dataframe: ", len(dummy_name_dataframe))
        #dummy_name_dataframe.reset_index(inplace=True)
        #dummy_name_dataframe.drop('#', axis=1, inplace=True)
        return dummy_name_dataframe

    def joinAdresses(self, features_dataframe):
        joinAdress_list = []
        for feature in zip(*features_dataframe):
            joinAdress_list.append(', '.join([*feature]))
        return joinAdress_list

    def generateDummyAdress(self, features=['urban','sub_district','city']):
        population = list(self.postal_code_dataframe.index)
        index_list = self.random.choices(population, k=self.rows)
        ##print(index_list)
        choose_postal_code_dataframe = self.postal_code_dataframe.loc[index_list]
        features_dataframe = [choose_postal_code_dataframe[feature] for feature in features]
        dummy_adress_dataframe = pd.DataFrame({'adress': self.joinAdresses(features_dataframe),
                                               'province_code': choose_postal_code_dataframe['province_code'],
                                               'postal_code': choose_postal_code_dataframe['postal_code']
                                               })
        dummy_adress_dataframe.index = self.index
        #dummy_adress_dataframe.reset_index(inplace=True)
        #dummy_adress_dataframe.drop('id', axis=1, inplace=True)
        return dummy_adress_dataframe

    def randomString(self, range_string=(0,5), weights=None, k=1):
        if type(range_string) == int:
            range_string = (0, range_string)
        if weights == None:
            n_population = range_string[1] - range_string[0]
            weights = [1/n_population for i in range(*range_string)]

        string_addition = '_' + string.ascii_lowercase + string.digits
        n_additions = self.random.choices(range(*range_string), weights=weights, k=k)
        choose_string_addition = [self.random.choices(string_addition, k=n_addition) for n_addition in n_additions]
        choose_string_addition = [''.join(addition) for addition in choose_string_addition]
        return choose_string_addition

    def generateDummyEmail(self, names):
        choose_email_domain = self.random.choices(self.email_domain, k=self.rows)
        choose_string_addition = self.randomString(range_string=4, weights=[10,35,35,20], k=self.rows)
        
        if self.last_name == True:
            zip_list = [names['first_name'].values, names['last_name'].values, choose_string_addition, choose_email_domain]
            email_list = []
            for first_name,last_name,addition,domain in zip(*zip_list):
                identity_probability = [first_name, last_name, f"{first_name}{last_name}", f"{last_name}{first_name}"]
                identity = self.random.choice(identity_probability)
                email_list.append(f"{identity.lower()}{addition}@{domain}")
            dummy_email_dataframe = pd.DataFrame({'email': email_list})
        else:
            zip_list = [names['name'].values, choose_string_addition, choose_email_domain]
            email_list = [f"{name.lower()}{addition}@{domain}" for name,addition,domain in zip(*zip_list)]
            dummy_email_dataframe = pd.DataFrame({'email': email_list})
        dummy_email_dataframe.index = self.index
        return dummy_email_dataframe

    def random_date(self, add_proportion, date_range, time_format):
        start_time = time.mktime(time.strptime(date_range[0], time_format))
        end_time = time.mktime(time.strptime(date_range[1], time_format))

        choice_time = start_time + add_proportion*(end_time - start_time)
        return time.strftime(time_format, time.localtime(choice_time))

    def age_calculator(self, date_of_birth, time_format):
        birthdate = datetime.strptime(date_of_birth, time_format)
        today = date.today()
        age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
        return age

    def generateDummyBirthday(self, date_range=('1 January 1980', '31 December 1999'), time_format="%d %B %Y"):
        birthday_list = [self.random_date(random.random(), date_range, time_format) for i in range(self.rows)]
        age_list = [self.age_calculator(date, time_format) for date in birthday_list]
        dummy_birthday_dataframe = pd.DataFrame({'birthday': birthday_list,
                                                 'age': age_list})
        return dummy_birthday_dataframe

    def generateMaritalStatus(self):
        status_list = ['single', 'married', 'divorced']
        weights = [60, 30, 10]
        dummy_marital_status = random.choices(status_list, weights=weights, k=self.rows)
        return dummy_marital_status

    def generateDummyIncome(self, income_range=(0,50000000,50000), prefix='Rp'):
        dummy_income = self.random.choices(range(*income_range), k=self.rows)
        prefix_variations = ['',' ','.']
        prefix_dummy_income = [f"{prefix}{random.choice(prefix_variations)}{income}" for income in dummy_income]
        return prefix_dummy_income

    def generateDummyLastEducation(self):
        education_list = ["High school graduate", "Bachelor's degree", "Master's degree", "Doctorate degree"]
        weights = [10, 70, 15, 5]
        dummy_last_education = self.random.choices(education_list, weights=weights, k=self.rows)
        return dummy_last_education

    def generateDataset(self, rows=100, last_name=True):
        self.rows = rows
        self.index = range(self.rows)
        self.last_name = last_name
        ##print('sukses 0')
        dummy_name_dataframe = self.generateDummyName()
        ##print(dummy_name_dataframe)
        #print("len dummy_name_dataframe 2: ", len(dummy_name_dataframe))
        dummy_adress_dataframe = self.generateDummyAdress()
        ##print(dummy_adress_dataframe)
        #print("len dummy_adress_dataframe 3: ", len(dummy_adress_dataframe))
        dummyDataset = pd.merge(dummy_name_dataframe, dummy_adress_dataframe, left_index=True, right_index=True)
        ##print('sukses 1')
        #print("len dummyDataset 4: ", len(dummyDataset))
        if self.last_name == True:
            names = dummy_name_dataframe[['first_name','last_name']]
            dummy_email_dataframe = self.generateDummyEmail(names)
        else:
            names = dummy_name_dataframe[['name']]
        dummy_email_dataframe = self.generateDummyEmail(names)
        dummyDataset = pd.merge(dummyDataset, dummy_email_dataframe, left_index=True, right_index=True)
        #print("len dummyDataset 5: ", len(dummyDataset))
        ##print('sukses 2')
        dummy_birthday_dataframe = self.generateDummyBirthday()
        dummyDataset = pd.concat([dummyDataset, dummy_birthday_dataframe], axis=1)
        ##print('sukses 3')
        dummy_marital_status = self.generateMaritalStatus()
        dummy_income = self.generateDummyIncome()
        dummy_last_education = self.generateDummyLastEducation()
        dummyDataset = dummyDataset.join(pd.DataFrame({'marital_status': dummy_marital_status,
                                                       'income_per_month': dummy_income,
                                                       'last_education': dummy_last_education
                                                       })) 
        return dummyDataset

#generator = DatasetGenerator(seed=101)
#dataframe = generator.generateDataset(last_name=False)
#print(dataframe[dataframe['age'].isnull() == True])
#print(dataframe.isnull().sum())