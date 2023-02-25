from SPARQLWrapper import SPARQLWrapper, JSON


class Wikidata:
    def query(self, query: str):
        sparql = SPARQLWrapper(
            "https://query.wikidata.org/sparql",
            agent="https://gridstats.russss.dev fetcher (russ@garrett.co.uk)",
        )
        sparql.setReturnFormat(JSON)

        sparql.setQuery(query)
        return sparql.queryAndConvert()

    def get_plants(self):
        ret = self.query(
            """
        SELECT DISTINCT ?item ?itemLabel ?bmrs_id WHERE {
        ?item wdt:P11610 ?bmrs_id.
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
        """
        )

        for result in ret["results"]["bindings"]:
            yield {
                "id": result["item"]["value"].split("/")[-1],
                "bmrs_id": result["bmrs_id"]["value"],
                "name": result["itemLabel"]["value"],
            }
