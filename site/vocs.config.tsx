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
          { text: 'Getting started', link: '/getting-started' },
          { text: 'Whitepaper', link: '/whitepaper' },
        ],
      },
      {
        text: 'Guides',
        items: [
          { text: 'Run Snapchain on AWS', link: '/guides/running-a-node' },
          { text: 'Make a new post using Snapchain', link: '/guides/writing-messages' },
          { text: 'Sync Snapchain to Postgres', link: '/guides/syncing-to-db' },
          { text: 'Migrate to Snapchain', link: '/guides/migrating-to-snapchain' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { 
            text: 'Datatypes', 
            items: [
              { text: 'Messages', link: '/reference/datatypes/messages' },
              { text: 'Events', link: '/reference/datatypes/events' },
            ],
            collapsed: true
          },
          { 
            text: 'GRPC API', 
            items: [
              { text: 'Using GRPC APIs', link: '/reference/grpcapi/grpcapi' },
              { text: 'Casts API', link: '/reference/grpcapi/casts' },
              { text: 'Reactions API', link: '/reference/grpcapi/reactions' },
              { text: 'Links API', link: '/reference/grpcapi/links' },
              { text: 'User Data API', link: '/reference/grpcapi/userdata' },
              { text: 'Username Proofs API', link: '/reference/grpcapi/usernameproof' },
              { text: 'Verifications API', link: '/reference/grpcapi/verification' },
              { text: 'Message API', link: '/reference/grpcapi/message' },
              { text: 'Fid API', link: '/reference/grpcapi/fids' },
              { text: 'Storage API', link: '/reference/grpcapi/storagelimits' },
              { text: 'Blocks API', link: '/reference/grpcapi/blocks' },
              { text: 'Onchain API', link: '/reference/grpcapi/onchain' },
              { text: 'Events API', link: '/reference/grpcapi/events' },
              { text: 'Metadata API', link: '/reference/grpcapi/metadata' },
            ],
            collapsed: true
          },
          { 
            text: 'HTTP API', 
            items: [
              { text: 'Using HTTP APIs', link: '/reference/httpapi/httpapi' },
              { text: 'Casts API', link: '/reference/httpapi/casts' },
              { text: 'Reactions API', link: '/reference/httpapi/reactions' },
              { text: 'Links API', link: '/reference/httpapi/links' },
              { text: 'User Data API', link: '/reference/httpapi/userdata' },
              { text: 'Username Proofs API', link: '/reference/httpapi/usernameproof' },
              { text: 'Verifications API', link: '/reference/httpapi/verification' },
              { text: 'Message API', link: '/reference/httpapi/message' },
              { text: 'Fid API', link: '/reference/httpapi/fids' },
              { text: 'Storage API', link: '/reference/httpapi/storagelimits' },
              { text: 'Onchain API', link: '/reference/httpapi/onchain' },
              { text: 'Events API', link: '/reference/httpapi/events' },
            ],
            collapsed: true
          },
        ],
      },
    ],
  socials: [
    {
      icon: 'github',
      link: 'https://github.com/farcasterxyz/snapchain',
    },
    {
      icon: 'x',
      link: 'https://x.com/farcaster_xyz',
    },
  ],
})