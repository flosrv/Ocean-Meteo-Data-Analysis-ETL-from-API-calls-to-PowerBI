import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import MDS
from sklearn.cluster import KMeans
from mpl_toolkits.mplot3d import Axes3D

######################### Analyses Univariées #############################################

# - Distribution des variables continues:
def plot_histogram(df, column_name):
    df[column_name].plot(kind='hist', bins=30, color='blue', edgecolor='black')

# - Statistiques descriptives:
def describe_column(df, column_name):
    return df[column_name].describe()

# - Boxplot:
def boxplot(df, column_name):
    sns.boxplot(x=df[column_name])

# - Densité (Kernel Density Estimation - KDE):
def kde_plot(df, column_name):
    sns.kdeplot(df[column_name], shade=True, color='blue')

# - Countplot pour variables catégorielles:
def countplot(df, column_name):
    sns.countplot(x=column_name, data=df)

######################### Analyses Bivariées #############################################

# - Corrélation:
def plot_correlation_heatmap(df, column1, column2):
    sns.heatmap(df[[column1, column2]].corr(), annot=True, cmap='coolwarm')

# - Nuage de points (scatter plot):
def scatter_plot(df, column1, column2):
    sns.scatterplot(x=df[column1], y=df[column2])

# - Boxplot comparatif:
def boxplot_comparatif(df, column1, column2):
    sns.boxplot(x=column1, y=column2, data=df)

# - Heatmap de la corrélation entre deux variables:
def heatmap_correlation(df, column1, column2):
    sns.heatmap(df[[column1, column2]].corr(), annot=True, cmap='coolwarm')

# - Diagramme de dispersion (scatter) avec colorisation par une autre variable:
def scatter_plot_with_hue(df, column1, column2, column3):
    sns.scatterplot(x=df[column1], y=df[column2], hue=df[column3])

################# Analyses Multivariées #############################################

# - Matrice de corrélation:
def correlation_matrix(df):
    sns.heatmap(df.corr(), annot=True, cmap='coolwarm')

# - Pairplot:
def pairplot(df, hue_column):
    sns.pairplot(df, hue=hue_column)

# - Régression linéaire:
def regplot(df, column1, column2):
    sns.regplot(x=column1, y=column2, data=df)

# - Graphique en barres groupées:
def barplot(df, column1, column2):
    sns.barplot(x=column1, y=column2, data=df)

# - Graphique en violon:
def violinplot(df, column1, column2):
    sns.violinplot(x=column1, y=column2, data=df)

# - Heatmap pour plusieurs variables catégorielles:
def heatmap_categorical(df, column1, column2):
    sns.heatmap(df.groupby([column1, column2]).size().unstack(), annot=True, cmap='Blues')

# - FacetGrid pour visualisation conditionnelle:
def facet_grid(df, col_column, hue_column, x, y):
    g = sns.FacetGrid(df, col=col_column, hue=hue_column)
    g.map(sns.scatterplot, x, y)
    g.add_legend()

# - MDS (Multidimensional Scaling):
def mds_plot(df):
    mds = MDS(n_components=2)
    mds_components = mds.fit_transform(df.select_dtypes(include=['float64', 'int64']))
    sns.scatterplot(x=mds_components[:, 0], y=mds_components[:, 1])

# - PCA (Analyse en Composantes Principales):
def pca_plot(df):
    pca = PCA(n_components=2)
    pca_components = pca.fit_transform(df.select_dtypes(include=['float64', 'int64']))
    sns.scatterplot(x=pca_components[:, 0], y=pca_components[:, 1])

# - Clustering (K-Means ou autres):
def kmeans_clustering(df, x_col, y_col, n_clusters=3):
    features = df.select_dtypes(include=['float64', 'int64'])
    kmeans = KMeans(n_clusters=n_clusters)
    df_copy = df.copy()
    df_copy['cluster'] = kmeans.fit_predict(features)
    sns.scatterplot(x=x_col, y=y_col, hue='cluster', data=df_copy)

# - 3D Scatter Plot:
def scatter_3d(df, column1, column2, column3):
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(df[column1], df[column2], df[column3])
    ax.set_xlabel(column1)
    ax.set_ylabel(column2)
    ax.set_zlabel(column3)
    plt.show()

# - Heatmap de la relation entre plusieurs variables:
def heatmap_multiple_variables(df):
    sns.heatmap(df.corr(), annot=True, cmap='YlGnBu')
