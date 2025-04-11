import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import MDS
from sklearn.cluster import KMeans
from mpl_toolkits.mplot3d import Axes3D

######################### Analyses Univariées #############################################

# - Distribution des variables continues:
#   # Exemple d'utilité: Visualiser la distribution des âges, salaires, etc.
def plot_histogram(df, column_name):
    df[column_name].compute().plot(kind='hist', bins=30, color='blue', edgecolor='black')

# - Statistiques descriptives:
#   # Exemple d'utilité: Obtenir les statistiques de base (moyenne, écart-type, etc.)
def describe_column(df, column_name):
    return df[column_name].describe().compute()

# - Boxplot:
#   # Exemple d'utilité: Identifier les outliers dans une distribution de données continues.
def boxplot(df, column_name):
    sns.boxplot(x=df[column_name].compute())

# - Densité (Kernel Density Estimation - KDE):
#   # Exemple d'utilité: Visualiser la densité de probabilité d'une variable continue.
def kde_plot(df, column_name):
    sns.kdeplot(df[column_name].compute(), shade=True, color='blue')

# - Countplot pour variables catégorielles:
#   # Exemple d'utilité: Visualiser la distribution des valeurs d'une variable catégorielle.
def countplot(df, column_name):
    sns.countplot(x=column_name, data=df.compute())

######################### Analyses Bivariées #############################################

# - Corrélation:
# Exemple d'utilité: Analyser la relation 
# linéaire entre deux variables continues.
def plot_correlation_heatmap(df, column1, column2):
    sns.heatmap(df[[column1, column2]].compute().corr(), annot=True, cmap='coolwarm')

## - Nuage de points (scatter plot):
# Exemple d'utilité: Visualiser la relation entre 
# deux variables continues.
def scatter_plot(df, column1, column2):
    sns.scatterplot(x=df[column1].compute(), y=df[column2].compute())

# - Boxplot comparatif:
# Exemple d'utilité: Comparer les distributions de plusieurs groupes 
# en fonction d'une variable continue.
def boxplot_comparatif(df, column1, column2):
    sns.boxplot(x=column1, y=column2, data=df.compute())

# - Heatmap de la corrélation entre deux variables:
# Exemple d'utilité: Visualiser la force de la relation 
# entre deux variables continues.
def heatmap_correlation(df, column1, column2):
    sns.heatmap(df[column1, column2].compute().corr(), annot=True, cmap='coolwarm')

# - Diagramme de dispersion (scatter) avec colorisation par une autre variable:
# Exemple d'utilité: Visualiser la relation entre deux 
# variables continues en utilisant une variable catégorielle comme couleur.
def scatter_plot_with_hue(df, column1, column2, column3):
    sns.scatterplot(x=df[column1].compute(), y=df[column2].compute(), hue=df['column3'].compute())

################# Analyses Multivariées #############################################

# - Matrice de corrélation:
# Exemple d'utilité: Visualiser les relations entre plusieurs variables continues.
def correlation_matrix(df):
    sns.heatmap(df.compute().corr(), annot=True, cmap='coolwarm')

# - Pairplot:
# Exemple d'utilité: Visualiser les relations entre 
# plusieurs variables continues et catégorielles.
def pairplot(df, hue_column):
    sns.pairplot(df.compute(), hue=hue_column)

# - Régression linéaire:
# Exemple d'utilité: Analyser et visualiser la relation 
# entre une variable dépendante et une ou plusieurs variables indépendantes.
def regplot(df, column1, column2):
    sns.regplot(x=column1, y=column2, data=df.compute())

# - Graphique en barres groupées:
# Exemple d'utilité: Comparer les moyennes ou 
# autres statistiques sur plusieurs groupes.
def barplot(df, column1, column2):
    sns.barplot(x=column1, y=column2, data=df.compute())

# - Graphique en violon:
# Exemple d'utilité: Comparer la distribution de 
# plusieurs groupes pour une variable continue.
def violinplot(df, column1, column2):
    sns.violinplot(x=column1, y=column2, data=df.compute())

# - Heatmap pour plusieurs variables catégorielles:
# Exemple d'utilité: Visualiser la fréquence 
# ou l'intensité de la relation entre deux variables catégorielles.
def heatmap_categorical(df, column1, column2):
    sns.heatmap(df.groupby([column1, 
    column2]).size().unstack().compute(), 
    annot=True, cmap='Blues')

# - FacetGrid pour visualisation conditionnelle:
# Exemple d'utilité: Créer des sous-graphes conditionnels 
# pour observer des relations dans des sous-groupes.
def facet_grid(df, col_column, hue_column, x, y):
    g = sns.FacetGrid(df.compute(), col=col_column, hue=hue_column)
    g.map(sns.scatterplot, x, y)
    g.add_legend()

# - MDS (Multidimensional Scaling):
# Exemple d'utilité: Réduire les dimensions des données 
# pour visualiser des relations dans un espace 2D ou 3D.
def mds_plot(df):
    mds = MDS(n_components=2)
    mds_components = mds.fit_transform(df.compute().select_dtypes(include=['float64', 'int64']))
    sns.scatterplot(x=mds_components[:, 0], y=mds_components[:, 1])

# - PCA (Analyse en Composantes Principales):
# Exemple d'utilité: Réduire la dimensionnalité tout en 
# conservant la variance des données pour la visualisation.
def pca_plot(df):
    pca = PCA(n_components=2)
    pca_components = pca.fit_transform(df.compute().select_dtypes(include=['float64', 'int64']))
    sns.scatterplot(x=pca_components[:, 0], y=pca_components[:, 1])

# - Clustering (K-Means ou autres):
#  Exemple d'utilité: Segmenter des données en groupes 
# pour analyser les clusters.
def kmeans_clustering(df, x_col, y_col, n_clusters=3):
    features = df.compute().select_dtypes(include=['float64', 'int64'])
    kmeans = KMeans(n_clusters=n_clusters)
    df = df.compute().copy()
    df['cluster'] = kmeans.fit_predict(features)
    sns.scatterplot(x=x_col, y=y_col, hue='cluster', data=df)

# - 3D Scatter Plot:
# Exemple d'utilité: Visualiser les relations entre 
# trois variables continues.
def scatter_3d(df, column1, column2, column3):
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    ax.scatter(df[column1].compute(), df[column2].compute(), df[column3].compute())
    ax.set_xlabel(column1)
    ax.set_ylabel(column2)
    ax.set_zlabel(column3)
    plt.show()

# - Heatmap de la relation entre plusieurs variables:
# Exemple d'utilité: Voir les interactions complexes 
# entre plusieurs variables continues.
def heatmap_multiple_variables(df):
    sns.heatmap(df.compute().corr(), annot=True, cmap='YlGnBu')
