# Mise Flow 📦

AI-powered voice inventory management for small businesses. Named after "mise en place" - the culinary practice of having everything in its place.

## Features

- 🎤 **Voice Dictation** - AWS Transcribe for accurate speech-to-text
- 🤖 **AI Processing** - Gemini 2.5 Flash for understanding commands
- 📧 **Gmail Integration** - Send emails to suppliers directly
- 📊 **Google Sheets** - Read/write inventory spreadsheets
- ⚡ **Quick Actions** - One-click common tasks

## Setup

### 1. Clone and Install

```bash
git clone https://github.com/yourusername/mise-flow.git
cd mise-flow
npm install
```

### 2. Environment Variables

Copy `.env.example` to `.env` and fill in:

```bash
cp .env.example .env
```

### 3. Google Cloud Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or use existing)
3. Enable APIs:
   - Gmail API
   - Google Sheets API
4. Create OAuth 2.0 credentials:
   - Application type: Web application
   - Authorized redirect URIs:
     - `http://localhost:8080/auth/google/callback` (development)
     - `https://yourdomain.com/auth/google/callback` (production)
5. Copy Client ID and Secret to `.env`

### 4. Gemini API Key

1. Go to [Google AI Studio](https://aistudio.google.com/apikey)
2. Create API key
3. Add to `.env` as `GEMINI_API_KEY`

### 5. AWS Credentials

1. Go to [AWS IAM Console](https://console.aws.amazon.com/iam/)
2. Create user with `AmazonTranscribeFullAccess` policy
3. Generate access keys
4. Add to `.env`

### 6. Run

```bash
npm start
```

Visit `http://localhost:8080`

## Deployment (Railway)

1. Push to GitHub
2. Connect repo to [Railway](https://railway.app)
3. Add environment variables in Railway dashboard
4. Update `GOOGLE_REDIRECT_URI` to your Railway domain

## Usage

### Voice Commands

- "Add 10 bags of espresso beans"
- "We're low on oat milk, need to order more"
- "Check stock on cups and lids"
- "Send email to supplier about tomorrow's delivery"

### Spreadsheet Format

Your inventory spreadsheet should have columns:
| Item | Quantity | Unit | Min Stock | Supplier |
|------|----------|------|-----------|----------|
| Espresso Beans | 15 | bags | 5 | Bean Co |
| Oat Milk | 8 | cases | 10 | Dairy Alt |

## Tech Stack

- **Backend**: Node.js, Express, WebSocket
- **AI**: Google Gemini 2.5 Flash
- **Transcription**: AWS Transcribe (streaming)
- **Integrations**: Gmail API, Google Sheets API
- **Frontend**: Vanilla JS, CSS

## License

MIT
