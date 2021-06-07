import pandas as pd
import numpy as np
import wikipedia

df = pd.read_csv('tedx_dataset.csv')
main_speakers = df['main_speaker'].tolist()
main_speakers = np.unique(main_speakers)
descriptions = []

print("start")
for main_speaker in main_speakers:
    try:
        title = wikipedia.search(main_speaker)[0]
        summary = wikipedia.summary(title, sentences=1)
    except:
        summary = ''

    descriptions.append({'main_speaker': main_speaker, 'main_speaker_description': summary})

df = pd.DataFrame.from_records(descriptions)
df.to_csv('main_speaker_description_dataset.csv')
print('Done')

