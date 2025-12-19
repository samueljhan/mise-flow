# Mise Flow 📦

AI-powered voice inventory management for small businesses. Named after "mise en place" - the culinary practice of having everything in its place.

## Features

- 🎤 **Voice Dictation** - AWS Transcribe for accurate speech-to-text
- 🧠 **Dual AI Architecture** - ChatGPT interprets, Gemini executes Google operations
- 📧 **Gmail Integration** - Send emails to suppliers directly
- 📊 **Google Sheets** - Read/write inventory spreadsheets
- ⚡ **Smart Inventory Awareness** - AI sees your actual stock levels and can warn about shortages

## Architecture

```
User Input → ChatGPT (understands intent + sees sheet data)
                ↓
            Gemini (executes Google Sheets/Gmail operations)
                ↓
            ChatGPT (interprets results for user)
```

**Why two AIs?**
- **ChatGPT** excels at understanding natural language and complex reasoning
- **Gemini** excels at Google Workspace operations and is cost-effective
- ChatGPT has **full visibility** into your Google Sheets data, so it can validate orders, answer business questions, and give smart warnings

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

Required variables:
```env
# OpenAI (ChatGPT) - for interpretation
OPENAI_API_KEY=sk-...

# Google Gemini - for Google Workspace operations  
GEMINI_API_KEY=...

# Google OAuth
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_REDIRECT_URI=http://localhost:8080/auth/google/callback

# AWS Transcribe
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1

# App auth
APP_USERNAME=admin
APP_PASSWORD=...
```

### 3. OpenAI API Key (NEW)

1. Go to [OpenAI Platform](https://platform.openai.com/api-keys)
2. Create a new API key
3. Add to `.env` as `OPENAI_API_KEY`

### 4. Google Cloud Setup

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

### 5. Gemini API Key

1. Go to [Google AI Studio](https://aistudio.google.com/apikey)
2. Create API key
3. Add to `.env` as `GEMINI_API_KEY`

### 6. AWS Credentials

1. Go to [AWS IAM Console](https://console.aws.amazon.com/iam/)
2. Create user with `AmazonTranscribeFullAccess` policy
3. Generate access keys
4. Add to `.env`

### 7. Run

```bash
npm start
```

Visit `http://localhost:8080`

## Deployment (Railway)

1. Push to GitHub
2. Connect repo to [Railway](https://railway.app)
3. Add environment variables in Railway dashboard (including `OPENAI_API_KEY`)
4. Update `GOOGLE_REDIRECT_URI` to your Railway domain

## Usage

### Voice Commands

- "Order 100 pounds of Archives Blend"
- "Check inventory"
- "Create invoice for CED - 50lb Ethiopia"
- "What's our stock on Brazil?"

### Smart Features (ChatGPT + Sheet Awareness)

ChatGPT can see your actual inventory and answer questions like:
- "Do we have enough green coffee for this order?" → Validates against actual stock
- "Which invoices are unpaid?" → Analyzes invoice sheet
- "What should I reorder?" → Looks at current levels

### Roast Order Workflow

1. Say "order roast" or mention coffee names
2. ChatGPT parses your request and shows batch calculations
3. Validates against green coffee inventory (warns if low)
4. Confirms order → Gemini updates Sheets + creates Gmail draft

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/chat/process` | Main chat - ChatGPT interprets with sheet context |
| `POST /api/chat/query` | Ask analytical questions about your data |
| `POST /api/chat/smart-operation` | Full ChatGPT→Gemini→ChatGPT workflow |
| `POST /api/roast-order/parse` | Parse roast orders with inventory validation |
| `POST /api/inventory/sync` | Sync inventory to Google Sheets |

## Tech Stack

- **Backend**: Node.js, Express, WebSocket
- **AI (Interpretation)**: OpenAI ChatGPT (gpt-4o-mini)
- **AI (Google Ops)**: Google Gemini 2.5 Flash
- **Transcription**: AWS Transcribe (streaming)
- **Integrations**: Gmail API, Google Sheets API
- **Frontend**: Vanilla JS, CSS

## Troubleshooting

### ChatGPT not responding
- Check `OPENAI_API_KEY` is valid
- Verify billing is set up on OpenAI

### Gemini rate limited
- Built-in retry logic handles this
- Wait a moment and try again

### Google Sheets not syncing
- Ensure Google OAuth is connected (click "Connect Google")
- Check spreadsheet ID is correct

## License

MIT