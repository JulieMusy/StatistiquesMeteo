import pandas as pd



# Conversion excel -> DataFrame de Pandas
traduction_df = pd.read_excel( "https://raw.githubusercontent.com/JulieMusy/StatistiquesMeteo/main/doc_parametres_synop_168.xlsx" )

# Création du dictionnaire en key : value   <-  "colonne Mnémonique" : "colonne Descriptif"
DICT_COLUMN_NAMES = dict( zip( traduction_df["Mnémonique"] , traduction_df["Descriptif"] ))

DICT_COLUMN_NAMES.update( { 'rr'      + str(k)  : "Précipitations dans les " + str(k) +" dernières heures" for k in [ 1 , 3 , 6 , 12 , 24 ] } )
DICT_COLUMN_NAMES.update( { 'tn'      + str(k)  : "Température minimale sur "+ str(k) +" heures"           for k in [ 12 , 24 ] } )
DICT_COLUMN_NAMES.update( { 'tx'      + str(k)  : "Température maximale sur "+ str(k) +" heures"           for k in [ 12 , 24 ] } ) 
DICT_COLUMN_NAMES.update( { 'phenspe' + str(k)  : "Phénomène spécial "       + str(k)                      for k in [ 1 , 2 , 3 , 4 ] } ) 
DICT_COLUMN_NAMES.update( { 'nnuage'  + str(k)  : "Nébulosité cche nuageuse "+ str(k)                      for k in [ 1 , 2 , 3 , 4 ] } ) 
DICT_COLUMN_NAMES.update( { 'ctype'   + str(k)  : "Type nuage               "+ str(k)                      for k in [ 1 , 2 , 3 , 4 ] } ) 
DICT_COLUMN_NAMES.update( { 'hnuage'  + str(k)  : "Hauteur de base "         + str(k)                      for k in [ 1 , 2 , 3 , 4 ] } ) 



ID_STATIONS_DF = pd.read_csv("https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/postesSynop.csv", sep=';')
DICT_ID_VERS_NOM = dict( zip( ID_STATIONS_DF["ID"] , ID_STATIONS_DF["Nom"] ) )


def getDataOneMonth( YYYYMM ) :
  # I)2) Téléchargement
  df = pd.read_csv('https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.'+str(YYYYMM)+".csv.gz",sep=';') 
  print(YYYYMM)
  # II)2) Application du dictionnaire sur le nom des colonnes
  df = df.rename( columns = DICT_COLUMN_NAMES )    

  # II)4) Destruction de la colonne fantôme
  df.drop( ['Unnamed: 59'] , axis=1 , inplace=True)   
  
  # III)3) Ajout d'une colonne de Nom de station
  df["Nom de la station"] = df[ "Indicatif OMM station"]
  df = df.replace({"Nom de la station": DICT_ID_VERS_NOM})

  # III)4) Réorganisation des colonnes
  premières_colonnes = [
    "Indicatif OMM station", "Nom de la station" , "Date (UTC)",                                  # Indexation et localisation
    "Température","Point de rosée","Température du thermomètre mouillé",	                        # Températures
    "Température minimale du sol sur 12 heures", "Température minimale sur 12 heures",
    "Température minimale sur 24 heures","Température maximale sur 12 heures",
    "Température maximale sur 24 heures",
    "Pression au niveau mer","Pression station",                                                  # Pressions
    "Précipitations dans les 1 dernières heures", "Précipitations dans les 3 dernières heures",   # Précipitations
    "Précipitations dans les 6 dernières heures", "Précipitations dans les 12 dernières heures",
    "Précipitations dans les 24 dernières heures",
    "Direction du vent moyen 10 mn","Vitesse du vent moyen 10 mn" ,                               # Vents
    "Rafales sur les 10 dernières minutes", "Rafales sur une période"
  ]
  reste_des_colonnes = list( df.columns.difference( premières_colonnes ) )
  toutes_les_colonnes = premières_colonnes + reste_des_colonnes
  df = df[ toutes_les_colonnes ]

  # IV)1) Typer les données
  df[ "Date (UTC)" ] = pd.to_datetime(  df[ "Date (UTC)" ] , format='%Y%m%d%H%M%S'  )
  colonnes_numériques = df.columns.difference( [ "Date (UTC)" , "Nom de la station" ] )
  for colonne in colonnes_numériques :
    df[ colonne ] = pd.to_numeric( df[ colonne ] , errors='coerce' )

  # IV)3) Redressement des données
  df[ ["Température",
      "Température du thermomètre mouillé",
      "Température minimale du sol sur 12 heures",
      "Température minimale sur 12 heures",
      "Température minimale sur 24 heures",
      "Température maximale sur 12 heures",
      "Température maximale sur 24 heures",
      "Point de rosée"] ] -= 273.15

  # IV)4) Indexation temporelle
  df.index = df[ "Date (UTC)" ]
  df.index.rename("Horodatage", inplace=True)

  return df



def getDataPeriod( fromYYYYMM , toYYYYMM ) :

# Première partie : Téléchargement et concaténation de toute la période
# cf V) ---------------------------------------------------------------

  # Convertion des bornes en dates
  date_start= pd.Timestamp( str( fromYYYYMM ) + "01" )
  date_stop = pd.Timestamp( str( toYYYYMM  ) + "01" ) + pd.DateOffset( months = 1 , hours = -3) 

  # Range
  dateRange = pd.date_range( date_start , date_stop , freq = 'MS') # Month Start convention pour inclure la dernière borne

  # Listing des DataFrame mensuels
  DFs = [ getDataOneMonth( date.strftime( '%Y%m' ) )  for date in dateRange ]

  # Fusion de tous les DataFrames téléchargés
  df_complet = pd.concat( DFs )

# Deuxième partie : Dégroupement par station, au format dictionnaire de DataFrame
# cf VI)1) ----------------------------------------------------------------------

  DFs_par_station =  { nom : données for ( nom , données ) in df_complet.groupby( "Nom de la station" ) }


# Troisième partie : Renforcement de l'indexation
# cf VII)2)--------------------------------------

  # Range de borne à borne toutes les 3 heures
  toutes_dates_3H = pd.date_range( date_start , date_stop , freq = '3H')


  # Boucle sur toutes les stations
  for station in DFs_par_station :
    df = DFs_par_station[ station ]

    # Détermination des index présent et manquants
    nos_dates_présentes  = df.index
    les_dates_manquantes = toutes_dates_3H . difference( nos_dates_présentes )

    # Création du df sparadrap sur les index manquants contenant toutes les colonnes
    sparadrap_df = pd.DataFrame(index= les_dates_manquantes , columns= df.columns )

    # Concaténation
    df = pd.concat( [ df , sparadrap_df ] )

    # Tri chronologique et réattribution du DataFrame soigné dans le dictionnaire de DFs
    DFs_par_station[ station ] = df . sort_index()

  return DFs_par_station 


def getDFs( ) :

  import gdown
  import lzma
  import pickle

  # Localisation du fichier sur le net
  #googleID = '1AOyu3IpOwkSZOMXGDwhhKIcxCU_xZoXy'
  googleID = '1eOmy1ISwnZFk5q516f_BoKDMmf-WR_FZ'
  url = 'https://drive.google.com/uc?id=' + googleID

  # Donner un nom au fichier créé une fois téléchargé ici
  #nomFichier = 'DFs_de_1996_à_2022.pickle.lzma'
  nomFichier = 'DFs_from199601_to202312.pickle.lzma'
  # Téléchargement du fichier sur un Drive Public ici dans notre session de travail
  gdown.download( url , nomFichier , quiet=False )

  with open("/content/DFs_de_1996_à_2022.pickle.lzma", "rb") as f:
      compressed_pickle = f.read()

  depressed_pickle = lzma.decompress( compressed_pickle )
  DFs = pickle.loads( depressed_pickle )  # turn bytes object back into data
  
  print( "Les données couvrent l'intégralité de 96 à 22" )
  
  return DFs

def downloadNewDFs( fromYYYYMM , toYYYYMM ) :
  
  DFs = getDataPeriod( fromYYYYMM , toYYYYMM )
  import lzma
  import pickle

  # Sérialisation & Compression
  with lzma.open("DFs_from"+fromYYYYMM+"_to"+toYYYYMM+".pickle.lzma", "wb") as fichier:
      pickle.dump(DFs, fichier)

  # Téléchargement
  from google.colab import files
  files.download("DFs_from"+fromYYYYMM+"_to"+toYYYYMM+".pickle.lzma") 
  
