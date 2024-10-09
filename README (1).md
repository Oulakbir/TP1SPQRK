# TP Spark - Analyse des Ventes par Ville

Ce projet contient deux applications Spark qui permettent de traiter des données de ventes issues d'un fichier texte. Le but est de calculer le total des ventes par ville, puis le total des ventes de produits par ville pour une année donnée. Ce projet a été développé dans le cadre de l'apprentissage de Spark et du traitement de données en Big Data.

## Structure du Projet

Le projet contient deux applications principales :

## 1. **Application 1 : Total des Ventes par Ville**  
   Cette application calcule le total des ventes dans chaque ville en utilisant un fichier texte contenant les ventes.  
   Format du fichier d'entrée `ventes.txt` :
   ```txt
2023-01-15 Paris Laptop 1200.50
2022-01-16 Lyon Smartphone 800.00
2021-02-10 Marseille Tablet 450.75
2023-03-05 Paris Headphones 150.25
2023-03-04 Paris Téléphone 2500.25
2022-03-12 Toulouse Camera 600.00
2021-04-18 Nice Smartwatch 250.00
2023-05-22 Bordeaux Printer 300.50
2022-06-30 Lille Monitor 350.00
2023-07-14 Strasbourg Keyboard 100.00
2023-08-09 Nantes Mouse 50.00
```
### 1.1 Classe TotalVentesParVille
 
```java
package ma.enset;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class TotalVentesParVille {
    public static void main(String[] args) {
        // Initialisation de la configuration Spark
        SparkConf conf = new SparkConf().setAppName("Total Sales by City");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Lecture du fichier texte contenant les ventes
        JavaRDD<String> lines = sc.textFile("ventes.txt");
        // Transformation : extraction des informations pertinentes (ville et prix)
        JavaPairRDD<String, Double> citySales = lines.mapToPair(line -> {
            String[] parts = line.split(" ");
            String city = parts[1];
            double price = Double.parseDouble(parts[3]);
            return new Tuple2<>(city, price);
        });
        // Réduction : calcul du total des ventes par ville
        JavaPairRDD<String, Double> totalSalesByCity = citySales.reduceByKey(Double::sum);
        // Affichage des résultats
        totalSalesByCity.foreach(tuple -> System.out.println(tuple._1() + " : " + tuple._2()));
        // Arrêt du contexte Spark
        sc.stop();
    }
}
```
### 1.2 Déploiement Local de l'Application Spark

![spark-1](https://github.com/user-attachments/assets/4d971b25-f36b-4366-8133-868a5f06aca8)

### 1.3 Déploiement de l'application Spark en mode Standalone

![spark-3](https://github.com/user-attachments/assets/81157173-48e1-4bea-a3d0-2052f2a005b8)

## 2. **Application 2 : Total des Ventes par Produit et Ville pour une Année Donnée**  
Cette application permet à l'utilisateur de spécifier une année donnée et de calculer le total des ventes des produits par ville pour cette année.

### 2.1 Classe VentesParAnnéeDonner

```java
package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Scanner;

public class VentesParAnnéeDonner {
    public static void main(String[] args) {
        // Demander à l'utilisateur de saisir l'année
        Scanner scanner = new Scanner(System.in);
        System.out.print("Veuillez entrer l'année : ");
        String annee = scanner.nextLine();

        // Initialisation de la configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("Total des Ventes par Produit et Ville");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lecture du fichier texte contenant les ventes
        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        // Filtrer les ventes pour l'année spécifiée
        JavaRDD<String> lignesFiltrees = lignes.filter(new org.apache.spark.api.java.function.Function<String, Boolean>() {
            @Override
            public Boolean call(String ligne) throws Exception {
                return ligne.startsWith(annee);
            }
        });

        // Transformation : extraire les informations pertinentes (ville, produit, prix)
        JavaPairRDD<Tuple2<String, String>, Double> ventesProduits = lignesFiltrees.mapToPair(new org.apache.spark.api.java.function.PairFunction<String, Tuple2<String, String>, Double>() {
            @Override
            public Tuple2<Tuple2<String, String>, Double> call(String ligne) throws Exception {
                String[] parties = ligne.split(" ");
                if (parties.length != 4) {
                    // Gestion des lignes mal formatées
                    System.err.println("Ligne mal formatée : " + ligne);
                    return new Tuple2<>(new Tuple2<>("Inconnu", "Inconnu"), 0.0);
                }
                String ville = parties[1];
                String produit = parties[2];
                double prix = 0.0;
                try {
                    prix = Double.parseDouble(parties[3]);
                } catch (NumberFormatException e) {
                    System.err.println("Prix invalide dans la ligne : " + ligne);
                }
                return new Tuple2<>(new Tuple2<>(ville, produit), prix);
            }
        });

        // Réduction : calculer le total des ventes par produit et par ville
        JavaPairRDD<Tuple2<String, String>, Double> totalVentesParProduitEtVille = ventesProduits
                .reduceByKey(new org.apache.spark.api.java.function.Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double v1, Double v2) throws Exception {
                        return v1 + v2;
                    }
                });

        // Affichage des résultats
        totalVentesParProduitEtVille.foreach(new org.apache.spark.api.java.function.VoidFunction<Tuple2<Tuple2<String, String>, Double>>() {
            @Override
            public void call(Tuple2<Tuple2<String, String>, Double> tuple) throws Exception {
                System.out.println("Ville : " + tuple._1()._1() +
                        ", Produit : " + tuple._1()._2() +
                        ", Total des Ventes : " + tuple._2());
            }
        });

        // Arrêt du contexte Spark
        sc.stop();
    }
}

```
### 2.2 Déploiement Local de l'Application Spark

![spark-2](https://github.com/user-attachments/assets/0a8bfe0a-21e9-49bb-bf96-b03ca8928406)

### 2.3 Déploiement de l'application Spark en mode Standalone

![spark-4](https://github.com/user-attachments/assets/3b2712c3-8e57-4543-867a-22fbacf33ab3)

## 3. L'interface de spark master 

![screencapture-localhost-8080-2024-10-08-23_59_51](https://github.com/user-attachments/assets/fb6ce04a-e2a9-4cef-87af-fdaecad2352b)
