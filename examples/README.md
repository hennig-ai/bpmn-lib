# Beispielmodelle

## bpmn-rule-coverage-example.md

Ein gueltiges BPMN-Modell, das **jede Regel des Regelkatalogs mindestens einmal zur
Auswertung bringt**. Es ist der Positivnachweis der Validierungspipeline: Laeuft es auf
Ebene `best_practice` ohne Fund durch, dann meldet keine Regel etwas Falsches — und keine
Regel schweigt, weil sie nichts zu pruefen gefunden haette.

Der zweite Punkt ist der eigentliche Grund fuer das Modell. Eine Regel, die kein Element
selektiert, meldet Erfolg, ohne hingesehen zu haben. Bevor es dieses Modell gab, waren 15
von 48 Regeln in genau diesem Zustand — nie ausgefuehrt, nie widerlegt.

Beide Zusagen sichert
[tests/integration/test_example_rule_coverage.py](../tests/integration/test_example_rule_coverage.py)
ab. Der Coverage-Test nennt beim Fehlschlag die Regeln, die kein Ziel gefunden haben.

## Woher Schema, Hierarchie und Regeln kommen

Das Modell ist eine reine Instanz. Zum Laden braucht es drei weitere Dateien, die im
Repository des **bpmn-modeling-Skills** liegen und dort auch gepflegt werden:

```
<skill>/references/
    bpmn-schema.md      Tabellen, Spalten, Constraints, Wertedomaenen
    bpmn-hierarchy.md   Vererbungshierarchie unter bpmn_element
    rules/*.md          Regelkatalog
```

Der Pfad kommt aus der Umgebungsvariablen `BPMN_METADATA_DIR`; ohne sie greift der im Test
hinterlegte Standardpfad. Fehlen die Dateien, ueberspringt sich der Coverage-Test, statt
fehlzuschlagen — ein Checkout ohne das Skill-Repository bleibt damit gruen.

Aufruf ueber das Validierungsskript des Skills:

```
python <skill>/scripts/validate_bpmn.py examples/bpmn-rule-coverage-example.md best_practice
```

## Abgrenzung zum Skill-Beispiel

`bpmn-instance-example.md` im Skill ist ein **Lehrbeispiel**: ein schlanker Rechnungsprozess,
an dem sich Modellierung ablesen laesst. Es bleibt bewusst einfach und soll nicht mit
Konstrukten befrachtet werden, die nur der Regelabdeckung dienen. Genau dafuer gibt es das
Modell hier.
