#### This function takes an input which only has two columns with the following definitions:
##   1- target: target column
##   2- variable whom effect on the target is questioned

def variable_transform(data_fr,target_column_name = 'target', params = {'max_depth':[4,6,8], 'min_samples_split':[250,1000,2000],'min_samples_leaf' : [250,2000,1000], 'max_leaf_nodes' :[6,8,10]}):

    from pylab import figure
    from sklearn.model_selection import GridSearchCV
    from matplotlib import pyplot as plt
    from sklearn import tree
    from sklearn.tree import DecisionTreeClassifier
    import pandas as pd
    import numpy as np
    
    
    ##  READ Data

    Y = data_fr[target_column_name]
    X = data_fr.drop([target_column_name], axis = 1)  
    
    fit = list()

    clf_dt = DecisionTreeClassifier()
    clf = GridSearchCV(clf_dt, param_grid=params, scoring='accuracy')
    clf.fit(X,Y)

    clf_dt.set_params(**clf.best_params_)
    clf_dt.fit(X,Y)

    fit.append(clf_dt.predict_proba(X))
    
    data_fr["node"] =  clf_dt.apply(X)

    result_table = data_fr.groupby(by = "node").agg({list(X.columns)[0]:['min', 'max'], 'target':['mean','count']}).sort_values([(list(X.columns)[0], 'max')], ascending=True)
    
    column_name  = list(set(data_fr.columns) - set(["node","target"]))
    
    result_table.columns = [str(column_name[0])+"_min",str(column_name[0])+"_max","target_rate","sample_count"]

    ## plot obj return ??
    ## def myplot():
    ##   fig = figure(figsize=(34,34))
    ##   x = tree.plot_tree(clf_dt, filled=True, feature_names = list(X.columns)[0], class_names=['0','1'],fontsize=20)
    ##   fig.savefig('plot.png') # This is just to show the figure is still generated
    ##   return fig
    
    
    return clf_dt, fit, result_table, clf.best_params_
