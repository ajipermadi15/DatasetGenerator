from datasetGenerator import DatasetGenerator, path_dictionary
import numpy as np
import re
import random

column_list = ['first_name', 'last_name', 'sex', 'nation', 'adress', 'province_code',
                'postal_code', 'email', 'birthday', 'age', 'marital_status',
                'income_per_month', 'last_education']
configDataset = {
                'missing_values':{'columns': ['last_name', 'province_code', 'birthday', 'sex'],
                                    'proportions': [0.3, 0.88, 0.3, 0.2]},
                'inconsistent': {'columns': ['marital_status', 'last_education', 'email', 'age'],
                                    'proportions': [0.2, 0.3, 0.1, 0.4]},
                'outlier': {'columns': ['age'],
                                    'proportions': [0.1]}
                }
configInconsistent = {
                        'marital_status': {'single':['lajang', 'belum menikah'],
                                            'married':['menikah'],
                                            'divorced':['cerai']},
                        'last_education': {"High school graduate": ['SMA', 'SMK'],
                                            "Bachelor's degree": ['S1', 'S-1'],
                                            "Master's degree": ['S2', 'S-2'], 
                                            "Doctorate degree": ['S3', "S-3"]}
                    }

class dirtyDatasetGenerator(DatasetGenerator):
    def __init__(self, seed=None, path_dictionary=path_dictionary):
        super().__init__(seed, path_dictionary)
    
    def missingValues(self, dataframe, proportions: list):
        for column, proportion in zip(dataframe.columns, proportions):
            n_missing = int(self.rows * proportion)
            idx_missing = self.random.sample(list(dataframe.index), k=n_missing)
            dataframe.loc[idx_missing, column] = np.NaN
        return dataframe

    def inconsistent(self, dataframe, proportions: list):
        for column, proportion in zip(dataframe.columns, proportions):
            n_inconsistent = int(self.rows * proportion)
            idx_inconsistent = self.random.sample(list(dataframe.index), k=n_inconsistent)
            if (dataframe[column].dtype == np.int64) or (dataframe[column].dtype == np.float64):
                dataframe.loc[idx_inconsistent, column] = dataframe.loc[idx_inconsistent, column].astype(str)
                #print(f'data type {column} = object')
            elif column == 'email':
                email_inconsistent_type = np.array([random.choice(range(2)) for i in range(n_inconsistent)])
                idx_type_0 = [idx for idx, type in zip(idx_inconsistent, email_inconsistent_type) if type==0]
                idx_type_1 = [idx for idx, type in zip(idx_inconsistent, email_inconsistent_type) if type==1]
                #print(email_inconsistent_type)
                replacement = [symbol for symbol in '+%^&#']
                dataframe.loc[idx_type_0, column] = dataframe.loc[idx_type_0, column].\
                    apply(lambda x: re.sub('@', random.choice(replacement), x))
                dataframe.loc[idx_type_1, column] = dataframe.loc[idx_type_1, column].\
                    apply(lambda x: self.randomString(range_string=(5,9), k=1)[0] + '@' + x.split('@')[-1])
                #print(f'data type {column} = email')
            elif column in configInconsistent.keys():
                #print(f'data type {column} in configInconsistent')
                dataframe.loc[idx_inconsistent, column] = dataframe.loc[idx_inconsistent, column].\
                    apply(lambda x: random.choice(configInconsistent[column][x]))
        return dataframe

    def outlier(self, dataframe, proportions: list):
        for column, proportion in zip(dataframe.columns, proportions):
            n_outlier = int(self.rows * proportion)
            idx_outlier = self.random.sample(list(dataframe.index), k=n_outlier)
            if (dataframe[column].dtype == np.int64) or (dataframe[column].dtype == np.float64):
                lower_limit = int(dataframe[column].max()/dataframe[column].min()*1.5)
                upper_limit = lower_limit + 2
                dataframe.loc[idx_outlier, column] = dataframe.loc[idx_outlier, column].\
                    apply(lambda x: random.choice([random.randint(lower_limit,upper_limit)*x, -x]))
        return dataframe

    def generateDataset(self, rows=100, last_name=True, configDataset=configDataset):
        dummyDataset = super().generateDataset(rows, last_name)
        #print(dummyDataset.info())
        missing_columns = configDataset['missing_values']['columns']
        missing_proportions = configDataset['missing_values']['proportions']
        missing_dataframe = self.missingValues(dummyDataset.loc[:,missing_columns], missing_proportions)
        dummyDataset.loc[:,missing_columns] = missing_dataframe
        #
        outlier_columns = configDataset['outlier']['columns']
        outlier_proportions = configDataset['outlier']['proportions']
        outlier_dataframe = self.outlier(dummyDataset.loc[:,outlier_columns], outlier_proportions)
        dummyDataset.loc[:,outlier_columns] = outlier_dataframe
        #print(dummyDataset.info())
        inconsistent_columns = configDataset['inconsistent']['columns']
        inconsistent_proportions = configDataset['inconsistent']['proportions']
        inconsistent_dataframe = self.inconsistent(dummyDataset.loc[:,inconsistent_columns], inconsistent_proportions)
        dummyDataset.loc[:,inconsistent_columns] = inconsistent_dataframe
        #
        #print(dummyDataset[['first_name', 'last_name', 'email', 'age', 'income_per_month']])
        #print(dummyDataset['age'].dtype)
        return dummyDataset

generator = dirtyDatasetGenerator(seed=101)
dataset = generator.generateDataset(rows=150)
dataset.to_csv('output/dataset_survey.csv')