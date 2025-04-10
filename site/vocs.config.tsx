import { defineConfig } from 'vocs'

export default defineConfig({
  font: {
    google: 'Inter',
  },
  title: 'Snapchain',
  theme: {
    accentColor: '#8a63d2',
  },
  sidebar: [
      {
        text: 'Overview',
        items: [
          { text: 'What is Snapchain?', link: '/' },
          { text: 'Getting Started', link: '/getting-started' },
        ],
      },
      {
        text: 'Guides',
        items: [
          { text: 'Running a node on AWS', link: '/guides/running-a-node' },
          { text: 'Reading messages from Snapchain', link: '/guides/reading-messages' },
          { text: 'Writing messages to Snapchain', link: '/guides/writing-messages' },
          { text: 'Syncing to a DB', link: '/guides/syncing-to-db' },
        ],
      },
    ],
})