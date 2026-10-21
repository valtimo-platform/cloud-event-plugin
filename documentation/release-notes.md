# Release notes

Overzicht van wijzigingen per versie van de Cloud Event-plugin.

## 0.1.7
Autoconfiguration van de Inbox wordt nu gedaan na het autoconfiguration van de listener in de plugin.

## 0.1.6

Binnenkomende cloud events worden betrouwbaarder afgehandeld:

- Een wachtende message catch event ontvangt het event nu ook achter een event-based gateway, en ook als de naam van
  het bericht een expressie bevat.
- Eén event zet elk wachtend proces precies één keer voort: niet twee gekoppelde stappen achter elkaar, en niet
  dubbel als een receive task een asynchrone voortzetting heeft. Parallelle takken worden allebei voortgezet.
- Een verkeerd ingestelde of opgeschorte koppeling wordt overgeslagen met een waarschuwing, in plaats van het event en
  alle andere koppelingen te laten mislukken.
- Een koppeling op een message start event heeft nu een event type filter nodig. Zonder filter zou elk event, ook die
  van Valtimo zelf, een nieuw dossier starten.
- Alleen events die bij een koppeling passen worden als verwerkt vastgelegd, en een event dat gelijktijdig dubbel
  binnenkomt wordt maar één keer verwerkt.

JSON die in het data-veld van de publish-actie wordt ingevuld, wordt nu als JSON verstuurd in plaats van als tekst.
Het gepubliceerde backend-artefact is nu een gewone jar, zodat ook Maven-projecten het kunnen gebruiken.

## 0.1.5

Spring Cloud Starter Stream Rabbit als API dependency toegevoegd.

## 0.1.4

Spring Cloud Starter Stream Rabbit dependency toegevoegd.

## 0.1.3

Ondersteuning voor Valtimo 13.48.0.

## 0.1.2

Een dossier dat door een binnenkomende cloud event wordt gestart, toont nu de gegevens van die
gebeurtenis in plaats van leeg te blijven.

## 0.1.1

Valtimo bijgewerkt naar versie 13.41.0.

## 0.1.0
Eerste release: CloudEvents versturen en ontvangen via RabbitMQ. Ondergebracht in een eigen repository met voorbeeldapplicatie en aparte documentatie.
