const express = require('express');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const OpenAI = require('openai');
const { google } = require('googleapis');
const cors = require('cors');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require("@aws-sdk/client-transcribe-streaming");
const { PassThrough } = require('stream');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 8080;
// Trust proxy for Railway (required for secure cookies behind proxy)
app.set('trust proxy', 1);

// Session configuration
const session = require('express-session');
app.use(session({
  secret: process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === 'production',
    httpOnly: true,
    sameSite: 'lax',
    maxAge: 7 * 24 * 60 * 60 * 1000 // 7 days
  }
}));

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Auth middleware - protects routes
function requireAuth(req, res, next) {
  if (req.session && req.session.authenticated) {
    return next();
  }
  // For API routes, return 401
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ error: 'Authentication required', redirect: '/login' });
  }
  // For page routes, redirect to login
  return res.redirect('/login');
}

// Serve login page (unprotected)
app.get('/login', (req, res) => {
  // If already authenticated, redirect to app
  if (req.session && req.session.authenticated) {
    return res.redirect('/');
  }
  res.send(getLoginPage());
});

// Password authentication
app.post('/auth/password', (req, res) => {
  const { username, password } = req.body;
  
  // Check against environment variables
  const validUser = process.env.APP_USERNAME || 'admin';
  const validPass = process.env.APP_PASSWORD;
  
  if (!validPass) {
    console.log('⚠️ APP_PASSWORD not set - password login disabled');
    return res.redirect('/login?error=password_disabled');
  }
  
  if (username === validUser && password === validPass) {
    req.session.authenticated = true;
    req.session.user = {
      name: username,
      email: '',
      authMethod: 'password'
    };
    console.log(`✅ Password auth successful: ${username}`);
    return res.redirect('/');
  }
  
  console.log(`❌ Password auth failed for: ${username}`);
  res.redirect('/login?error=invalid');
});

// Logout endpoint
app.get('/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/login');
});

// Auth routes (unprotected)
app.get('/auth/google', (req, res) => {
  const scopes = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.compose',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile'
  ];

  const url = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: scopes,
    prompt: 'consent'
  });

  res.redirect(url);
});

app.get('/auth/google/callback', async (req, res) => {
  const { code } = req.query;

  try {
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);
    userTokens = tokens;

    // Get user info
    const oauth2 = google.oauth2({ version: 'v2', auth: oauth2Client });
    const userInfo = await oauth2.userinfo.get();
    
    // Set session as authenticated
    req.session.authenticated = true;
    req.session.user = {
      email: userInfo.data.email,
      name: userInfo.data.name,
      picture: userInfo.data.picture
    };

    console.log(`✅ User authenticated: ${userInfo.data.email}`);
    
    // Format all sheets with currency formatting (async, don't wait)
    formatAllSheetsCurrency().catch(e => console.log('Currency format on connect:', e.message));
    
    // Explicitly save session before redirect
    req.session.save((err) => {
      if (err) {
        console.error('Session save error:', err);
        return res.redirect('/login?error=session_failed');
      }
      res.redirect('/');
    });
  } catch (error) {
    console.error('Auth callback error:', error);
    res.redirect('/login?error=auth_failed');
  }
});

// Health check (unprotected)
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'Mise Flow API',
    interpretation: 'ChatGPT (OpenAI)',
    googleOps: 'Gemini 2.5 Flash',
    transcription: 'AWS Transcribe',
    google: userTokens ? 'connected' : 'not connected'
  });
});

// Privacy page (unprotected)
app.get('/privacy', (req, res) => {
  res.send(getPrivacyPage());
});

// Get current user info
app.get('/api/user', requireAuth, (req, res) => {
  res.json({
    authenticated: true,
    user: req.session.user
  });
});

// Google connection status - required by frontend
app.get('/api/google/status', requireAuth, (req, res) => {
  res.json({ 
    connected: !!userTokens,
    hasRefreshToken: !!process.env.GOOGLE_REFRESH_TOKEN
  });
});

// Protect static files - serve login page if not authenticated
app.use((req, res, next) => {
  // Allow access to login-related assets
  if (req.path === '/login' || req.path.startsWith('/auth/') || req.path === '/privacy' || req.path === '/api/health') {
    return next();
  }
  
  // Check authentication for all other routes
  if (!req.session || !req.session.authenticated) {
    // For HTML pages, redirect to login
    if (req.accepts('html') && !req.path.startsWith('/api/')) {
      return res.redirect('/login');
    }
    // For API routes, return 401
    if (req.path.startsWith('/api/')) {
      return res.status(401).json({ error: 'Authentication required' });
    }
  }
  next();
});

app.use(express.static('public'));

// Login page HTML
function getLoginPage(error = '') {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Mise Flow - Login</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #1a1a1a;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .login-container {
      background: #2a2a2a;
      border-radius: 12px;
      padding: 40px;
      max-width: 360px;
      width: 90%;
    }
    h1 {
      color: #e0e0e0;
      font-size: 1.6rem;
      margin-bottom: 6px;
      font-weight: 600;
    }
    .subtitle {
      color: #888;
      margin-bottom: 28px;
      font-size: 0.85rem;
      line-height: 1.4;
    }
    .form-group {
      margin-bottom: 16px;
    }
    .form-group label {
      display: block;
      color: #aaa;
      font-size: 0.8rem;
      margin-bottom: 6px;
    }
    .form-group input {
      width: 100%;
      padding: 12px;
      background: #1a1a1a;
      border: 1px solid #3a3a3a;
      border-radius: 6px;
      color: #e0e0e0;
      font-size: 0.95rem;
    }
    .form-group input:focus {
      outline: none;
      border-color: #555;
    }
    .login-btn {
      width: 100%;
      padding: 12px;
      background: #3a3a3a;
      border: none;
      border-radius: 6px;
      color: #e0e0e0;
      font-size: 0.95rem;
      cursor: pointer;
      margin-bottom: 16px;
      transition: background 0.2s;
    }
    .login-btn:hover {
      background: #4a4a4a;
    }
    .divider {
      display: flex;
      align-items: center;
      margin: 20px 0;
      color: #555;
      font-size: 0.8rem;
    }
    .divider::before, .divider::after {
      content: '';
      flex: 1;
      height: 1px;
      background: #3a3a3a;
    }
    .divider span {
      padding: 0 12px;
    }
    .google-btn {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 10px;
      width: 100%;
      padding: 12px;
      background: #fff;
      border: none;
      border-radius: 6px;
      color: #333;
      font-size: 0.95rem;
      cursor: pointer;
      text-decoration: none;
      transition: background 0.2s;
    }
    .google-btn:hover {
      background: #f0f0f0;
    }
    .google-btn svg {
      width: 18px;
      height: 18px;
    }
    .error-msg {
      background: #442222;
      color: #ff6b6b;
      padding: 10px 12px;
      border-radius: 6px;
      margin-bottom: 20px;
      font-size: 0.85rem;
    }
    .footer {
      margin-top: 24px;
      text-align: center;
    }
    .footer a {
      color: #666;
      font-size: 0.75rem;
      text-decoration: none;
    }
    .footer a:hover {
      color: #888;
    }
  </style>
</head>
<body>
  <div class="login-container">
    <h1>Mise Flow</h1>
    <p class="subtitle">AI-powered Work Flow for AOU Coffee, Inc.</p>
    
    <div class="error-msg" id="errorMsg" style="display: none;">Authentication failed. Please try again.</div>
    
    <form action="/auth/password" method="POST">
      <div class="form-group">
        <label>User</label>
        <input type="text" name="username" autocomplete="username" required>
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" name="password" autocomplete="current-password" required>
      </div>
      <button type="submit" class="login-btn">Sign In</button>
    </form>
    
    <div class="divider"><span>or</span></div>
    
    <a href="/auth/google" class="google-btn">
      <svg viewBox="0 0 24 24">
        <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
        <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
        <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
        <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
      </svg>
      Sign in with Google
    </a>
    
    <div class="footer">
      <a href="/privacy">Privacy Policy</a>
    </div>
  </div>
  
  <script>
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.get('error')) {
      document.getElementById('errorMsg').style.display = 'block';
    }
  </script>
</body>
</html>`;
}

// Privacy page HTML (moved to function)
function getPrivacyPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Privacy Policy - Mise Flow</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #1a1a1a;
      color: #e0e0e0;
      line-height: 1.6;
      padding: 40px 20px;
    }
    .container {
      max-width: 800px;
      margin: 0 auto;
      background: #252525;
      border-radius: 12px;
      padding: 40px;
    }
    h1 {
      color: #fff;
      margin-bottom: 8px;
      font-size: 2rem;
    }
    .subtitle {
      color: #888;
      margin-bottom: 32px;
      font-size: 0.9rem;
    }
    h2 {
      color: #28a745;
      margin-top: 32px;
      margin-bottom: 12px;
      font-size: 1.2rem;
    }
    p, li {
      color: #ccc;
      margin-bottom: 12px;
    }
    ul {
      margin-left: 24px;
      margin-bottom: 16px;
    }
    a {
      color: #28a745;
      text-decoration: none;
    }
    a:hover {
      text-decoration: underline;
    }
    .back-link {
      display: inline-block;
      margin-top: 32px;
      padding: 10px 20px;
      background: #28a745;
      color: #fff;
      border-radius: 6px;
      text-decoration: none;
    }
    .back-link:hover {
      background: #218838;
      text-decoration: none;
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>Privacy Policy</h1>
    <p class="subtitle">Last updated: December 2024</p>
    
    <p>Mise Flow ("we", "our", or "us") is an inventory and invoice management tool for Archives of Us Coffee. This policy explains how we handle your information.</p>
    
    <h2>Information We Collect</h2>
    <p>When you use Mise Flow, we may collect:</p>
    <ul>
      <li><strong>Business Data:</strong> Customer names, invoice details, inventory records, and order information you enter into the system</li>
      <li><strong>Google Account Data:</strong> When you connect your Google account, we access Gmail (to draft emails) and Google Sheets (to sync data) with your permission</li>
      <li><strong>Voice Input:</strong> If you use voice dictation, audio is processed by AWS Transcribe and is not stored after transcription</li>
    </ul>
    
    <h2>How We Use Your Information</h2>
    <ul>
      <li>Generate and manage invoices</li>
      <li>Track coffee inventory (green, roasted, and en route)</li>
      <li>Sync data with your Google Sheets</li>
      <li>Draft emails in your Gmail account</li>
      <li>Process natural language commands via AI (Google Gemini)</li>
    </ul>
    
    <h2>Data Storage</h2>
    <ul>
      <li>Invoice PDFs are stored temporarily on our servers</li>
      <li>Inventory data syncs to your connected Google Sheet</li>
      <li>We do not sell or share your business data with third parties</li>
    </ul>
    
    <h2>Third-Party Services</h2>
    <p>Mise Flow uses the following services:</p>
    <ul>
      <li><strong>Google APIs:</strong> Gmail and Sheets integration (governed by <a href="https://policies.google.com/privacy" target="_blank">Google's Privacy Policy</a>)</li>
      <li><strong>AWS Transcribe:</strong> Voice-to-text processing</li>
      <li><strong>Google Gemini:</strong> AI-powered natural language processing</li>
      <li><strong>Railway:</strong> Application hosting</li>
    </ul>
    
    <h2>Your Rights</h2>
    <ul>
      <li>Disconnect your Google account at any time</li>
      <li>Request deletion of your data</li>
      <li>Export your invoice and inventory records</li>
    </ul>
    
    <h2>Contact</h2>
    <p>For privacy questions or data requests, contact: <a href="mailto:samueljhan@gmail.com">samueljhan@gmail.com</a></p>
    
    <a href="/login" class="back-link">← Back to Login</a>
  </div>
</body>
</html>`;
}

// Archives of Us Coffee Spreadsheet ID
const SPREADSHEET_ID = '1D5JuAEpOC2ZXD2IAel1ImBXqFUrcMzFY-gXu4ocOMCw';

// Create invoices directory if it doesn't exist
const invoicesDir = path.join(__dirname, 'public', 'invoices');
if (!fs.existsSync(invoicesDir)) {
  fs.mkdirSync(invoicesDir, { recursive: true });
}

// Gemini AI configuration
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// OpenAI (ChatGPT) configuration - for interpretation tasks
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

// Helper: Call ChatGPT for interpretation and conversational tasks
async function callChatGPT(systemPrompt, userMessage, options = {}) {
  const maxRetries = options.maxRetries || 3;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await openai.chat.completions.create({
        model: options.model || 'gpt-4o-mini',
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userMessage }
        ],
        temperature: options.temperature ?? 0.3,
        max_tokens: options.maxTokens || 1000,
        response_format: options.jsonMode ? { type: 'json_object' } : undefined
      });
      
      return response.choices[0].message.content.trim();
    } catch (error) {
      const isRateLimit = error.status === 429;
      
      if (isRateLimit && attempt < maxRetries) {
        const waitTime = attempt * 2000;
        console.log(`⏳ ChatGPT rate limited, waiting ${waitTime/1000}s before retry ${attempt + 1}/${maxRetries}...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      } else if (isRateLimit) {
        throw new Error('CHATGPT_RATE_LIMITED');
      } else {
        throw error;
      }
    }
  }
}

// Helper: Build sheet context for ChatGPT (gives it visibility into Google Sheets data)
async function buildSheetContextForChatGPT() {
  let context = {
    inventory: {
      green: greenCoffeeInventory,
      roasted: roastedCoffeeInventory,
      enRoute: enRouteCoffeeInventory
    },
    customers: customerDirectory,
    sheetData: null
  };
  
  // If Google is connected, fetch fresh sheet data for full context
  if (userTokens) {
    try {
      oauth2Client.setCredentials(userTokens);
      const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
      
      const [inventoryData, invoicesData] = await Promise.all([
        sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: 'Inventory!A:G'
        }).catch(() => ({ data: { values: [] } })),
        sheets.spreadsheets.values.get({
          spreadsheetId: SPREADSHEET_ID,
          range: 'Invoices!A:F'
        }).catch(() => ({ data: { values: [] } }))
      ]);
      
      context.sheetData = {
        inventory: inventoryData.data.values || [],
        invoices: invoicesData.data.values || []
      };
    } catch (error) {
      console.log('Could not fetch sheet data for context:', error.message);
    }
  }
  
  return context;
}

// Helper: Format sheet context as string for ChatGPT prompts
function formatSheetContextForPrompt(context) {
  let contextStr = `
=== CURRENT INVENTORY STATE ===

GREEN COFFEE (unroasted):
${context.inventory.green.map(c => `- ${c.name}: ${c.weight}lb (Profile: ${c.roastProfile}, Drop Temp: ${c.dropTemp}°F)`).join('\n')}

ROASTED COFFEE:
${context.inventory.roasted.map(c => {
  const recipe = c.recipe ? c.recipe.map(r => `${r.percentage}% ${r.name}`).join(' + ') : 'Private Label';
  return `- ${c.name}: ${c.weight}lb [${c.type}] Recipe: ${recipe}`;
}).join('\n')}

EN ROUTE (ordered, not yet received):
${context.inventory.enRoute.length > 0 
  ? context.inventory.enRoute.map(c => `- ${c.name}: ${c.weight}lb [Tracking: ${c.trackingNumber || 'pending'}]`).join('\n')
  : '- None'}

KNOWN CUSTOMERS:
${Object.values(context.customers).map(c => `- ${c.name} (${c.code}): ${c.emails?.join(', ') || 'no email'}`).join('\n')}
`;

  if (context.sheetData?.invoices?.length > 0) {
    contextStr += `\nRECENT INVOICES (last 10):\n`;
    const recentInvoices = context.sheetData.invoices.slice(-10);
    recentInvoices.forEach(row => {
      if (row[1]) contextStr += `- ${row.join(' | ')}\n`;
    });
  }

  return contextStr;
}

// Helper: Call Gemini with retry logic for rate limits
async function callGeminiWithRetry(prompt, options = {}) {
  const maxRetries = options.maxRetries || 3;
  const model = genAI.getGenerativeModel({ 
    model: options.model || "gemini-2.5-flash",
    generationConfig: { temperature: options.temperature || 0 }
  });
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await model.generateContent(prompt);
      return result.response.text().trim();
    } catch (error) {
      const isRateLimit = error.message?.includes('429') || error.message?.includes('quota');
      
      if (isRateLimit && attempt < maxRetries) {
        // Extract retry delay from error or use default
        const retryMatch = error.message?.match(/retry in (\d+)/i);
        const waitTime = retryMatch ? parseInt(retryMatch[1]) * 1000 : (attempt * 2000);
        console.log(`⏳ Rate limited, waiting ${waitTime/1000}s before retry ${attempt + 1}/${maxRetries}...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      } else if (isRateLimit) {
        throw new Error('RATE_LIMITED');
      } else {
        throw error;
      }
    }
  }
}

// Google OAuth configuration (Gmail + Sheets)
const oauth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.NODE_ENV === 'production' 
    ? process.env.GOOGLE_REDIRECT_URI || 'https://yourdomain.com/auth/google/callback'
    : 'http://localhost:8080/auth/google/callback'
);

// Token storage (use database for production)
let userTokens = null;

// Auto-load refresh token from environment if available
if (process.env.GOOGLE_REFRESH_TOKEN) {
  userTokens = {
    refresh_token: process.env.GOOGLE_REFRESH_TOKEN
  };
  oauth2Client.setCredentials(userTokens);
  console.log('📝 Google refresh token loaded from environment (will verify on first use)');
}

// AWS Transcribe configuration (standard, not medical)
const transcribeClient = new TranscribeStreamingClient({
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

console.log('=== Environment Check ===');
console.log('OpenAI:', !!process.env.OPENAI_API_KEY ? '✓' : '✗');
console.log('Gemini:', !!process.env.GEMINI_API_KEY ? '✓' : '✗');
console.log('Google Client ID:', !!process.env.GOOGLE_CLIENT_ID ? '✓' : '✗');
console.log('Google Client Secret:', !!process.env.GOOGLE_CLIENT_SECRET ? '✓' : '✗');
console.log('Google Refresh Token:', !!process.env.GOOGLE_REFRESH_TOKEN ? '✓ (auto-connect enabled)' : '✗ (manual auth required)');
console.log('AWS Access Key:', !!process.env.AWS_ACCESS_KEY_ID ? '✓' : '✗');
console.log('AWS Secret Key:', !!process.env.AWS_SECRET_ACCESS_KEY ? '✓' : '✗');
console.log('AWS Region:', process.env.AWS_REGION || 'us-east-1');
console.log('========================');

// System prompt for inventory assistant
const SYSTEM_PROMPT = `You are Mise Flow, an AI assistant for Archives of Us Coffee. Your name comes from "mise en place" - the culinary practice of having everything in its place.

You help with:
1. **Invoicing** - Create invoices for wholesale customers
2. **Inventory** - Track stock, log deliveries, set alerts
3. **Orders** - Document and process orders
4. **Communication** - Email suppliers and customers
5. **Reporting** - Generate reports and update spreadsheets

KNOWN WHOLESALE CUSTOMERS: Archives of Us, CED, Dex, Junia

SMART PATTERN RECOGNITION:
When you see a pattern like "[Customer] [Quantity] [Product]" (e.g., "CED 100 lbs Archives Blend"), recognize this as an invoice request.

Examples:
- "CED 100 lbs Archives Blend" → ACTION: create_invoice
- "Dex 50lb Ethiopia" → ACTION: create_invoice  
- "Maru Coffee 100lb archives blend" → ACTION: create_invoice (new customer - ask to confirm adding them)
- "Add 20 bags espresso to inventory" → ACTION: update_inventory
- "What's our stock on Colombia?" → ACTION: check_inventory
- "Email CED about their order" → ACTION: send_email

RESPONSE FORMAT:
When you identify an action, respond with:
1. A brief, friendly confirmation of what you understood
2. The action you'll take
3. DO NOT show raw JSON to the user - keep it conversational

For invoice patterns, respond like:
"Got it! Creating an invoice for [Customer] - [Quantity] of [Product]. Processing now..."

For unknown customers, respond like:
"I don't recognize [Customer] as a current wholesale client. Would you like me to add them as a new customer?"

Be concise, friendly, and action-oriented. Don't ask unnecessary questions - if the intent is clear, proceed with the action.`;

// Customer directory with codes and emails
let customerDirectory = {
  'archives of us': {
    name: 'Archives of Us',
    code: 'AOU',
    emails: ['nick@archivesofus.com']
  },
  'ced': {
    name: 'CED',
    code: 'CED',
    emails: ['songs0519@hotmail.com']
  },
  'dex': {
    name: 'Dex',
    code: 'DEX',
    emails: ['dexcoffeeusa@gmail.com', 'lkusacorp@gmail.com']
  },
  'junia': {
    name: 'Junia',
    code: 'JUN',
    emails: ['hello@juniacafe.com']
  }
};

// Helper to get known customers list
function getKnownCustomers() {
  return Object.values(customerDirectory).map(c => c.name);
}

// Helper to get customer code
function getCustomerCode(customerName) {
  const lower = customerName.toLowerCase();
  if (customerDirectory[lower]) {
    return customerDirectory[lower].code;
  }
  // Default: first 3 letters uppercase
  return customerName.substring(0, 3).toUpperCase();
}

// Helper to get customer emails
function getCustomerEmails(customerName) {
  const lower = customerName.toLowerCase();
  if (customerDirectory[lower]) {
    return customerDirectory[lower].emails || [];
  }
  return [];
}

// Helper to add/update customer
function addOrUpdateCustomer(name, code, emails = []) {
  const lower = name.toLowerCase();
  customerDirectory[lower] = {
    name: name,
    code: code.toUpperCase(),
    emails: emails
  };
}

// ============ Coffee Inventory Data ============

// Green Coffee Inventory (unroasted beans)
// Note: Ethiopia Gera has TWO lots (58484, 58479) that should be split 50/50 for each batch
let greenCoffeeInventory = [
  {
    id: 'colombia-antioquia',
    name: 'Colombia Antioquia',
    weight: 100,
    roastProfile: '122302',
    dropTemp: 410
  },
  {
    id: 'ethiopia-gera-58484',
    name: 'Ethiopia Gera 58484',
    weight: 50,
    roastProfile: '061901',
    dropTemp: 414
  },
  {
    id: 'ethiopia-gera-58479',
    name: 'Ethiopia Gera 58479',
    weight: 50,
    roastProfile: '061901',
    dropTemp: 414
  },
  {
    id: 'brazil-mogiano',
    name: 'Brazil Mogiano',
    weight: 400,
    roastProfile: '199503',
    dropTemp: 419
  },
  {
    id: 'ethiopia-yirgacheffe',
    name: 'Ethiopia Yirgacheffe',
    weight: 100,
    roastProfile: '141402',
    dropTemp: 415
  }
];

// Roasted Coffee Inventory
// Archives Blend: 66.67% Brazil Mogiano + 33.33% Ethiopia Yirgacheffe
// Ethiopia Gera: 50% lot 58484 + 50% lot 58479 (both roasted together per batch)
let roastedCoffeeInventory = [
  {
    id: 'archives-blend',
    name: 'Archives Blend',
    weight: 150,
    type: 'Blend',
    recipe: [
      { greenCoffeeId: 'brazil-mogiano', name: 'Brazil Mogiano', percentage: 66.6667 },
      { greenCoffeeId: 'ethiopia-yirgacheffe', name: 'Ethiopia Yirgacheffe', percentage: 33.3333 }
    ]
  },
  {
    id: 'ethiopia-gera-roasted',
    name: 'Ethiopia Gera',
    weight: 40,
    type: 'Single Origin',
    // Special: Ethiopia Gera uses TWO lots split 50/50 per batch
    recipe: [
      { greenCoffeeId: 'ethiopia-gera-58484', name: 'Ethiopia Gera 58484', percentage: 50 },
      { greenCoffeeId: 'ethiopia-gera-58479', name: 'Ethiopia Gera 58479', percentage: 50 }
    ],
    specialInstructions: 'Split 50/50 between lot 58484 and 58479 for each batch'
  },
  {
    id: 'colombia-excelso',
    name: 'Colombia Excelso',
    weight: 50,
    type: 'Single Origin',
    recipe: [
      { greenCoffeeId: 'colombia-antioquia', name: 'Colombia Antioquia', percentage: 100 }
    ]
  },
  {
    id: 'colombia-decaf',
    name: 'Colombia Decaf',
    weight: 30,
    type: 'Private Label',
    recipe: null // N/A - no green coffee inventory for private label
  }
];

// En Route Coffee Inventory (ordered but not yet received)
let enRouteCoffeeInventory = [];

// Helper to get green coffee by ID
function getGreenCoffee(id) {
  return greenCoffeeInventory.find(c => c.id === id);
}

// Helper to get roasted coffee by ID
function getRoastedCoffee(id) {
  return roastedCoffeeInventory.find(c => c.id === id);
}

// Helper to format inventory summary
function formatInventorySummary() {
  let summary = '**☕ Current Coffee Inventory**\n\n';
  
  summary += '**🌿 Green Coffee (Unroasted):**\n';
  greenCoffeeInventory.forEach(coffee => {
    summary += `• ${coffee.name}: ${coffee.weight}lb (Profile: ${coffee.roastProfile}, Drop: ${coffee.dropTemp}°F)\n`;
  });
  
  summary += '\n**🔥 Roasted Coffee:**\n';
  roastedCoffeeInventory.forEach(coffee => {
    let recipeStr = '';
    if (coffee.recipe) {
      recipeStr = coffee.recipe.map(r => `${r.percentage}% ${r.name}`).join(' + ');
    } else {
      recipeStr = 'N/A';
    }
    summary += `• ${coffee.name}: ${coffee.weight}lb [${coffee.type}] - Recipe: ${recipeStr}\n`;
  });
  
  if (enRouteCoffeeInventory.length > 0) {
    summary += '\n**🚚 En Route (Ordered):**\n';
    enRouteCoffeeInventory.forEach(coffee => {
      const tracking = coffee.trackingNumber || 'No tracking yet';
      summary += `• ${coffee.name}: ${coffee.weight}lb [${coffee.type}] - Tracking: ${tracking}\n`;
    });
  } else {
    summary += '\n**🚚 En Route:** None\n';
  }
  
  return summary;
}

// ============ Sheet Formatting Helpers ============

// Apply currency format ($X.XX) to specific columns in a sheet
async function applyCurrencyFormat(sheets, sheetName, columns, startRow = 3) {
  try {
    // Get sheet ID
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId: SPREADSHEET_ID
    });
    const sheet = spreadsheet.data.sheets.find(s => s.properties.title === sheetName);
    
    if (!sheet) {
      console.log(`Sheet "${sheetName}" not found for currency formatting`);
      return;
    }
    
    const sheetId = sheet.properties.sheetId;
    
    // Build format requests for each column
    const requests = columns.map(colIndex => ({
      repeatCell: {
        range: {
          sheetId: sheetId,
          startRowIndex: startRow - 1, // 0-indexed
          startColumnIndex: colIndex,
          endColumnIndex: colIndex + 1
        },
        cell: {
          userEnteredFormat: {
            numberFormat: {
              type: 'CURRENCY',
              pattern: '"$"#,##0.00'
            }
          }
        },
        fields: 'userEnteredFormat.numberFormat'
      }
    }));
    
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests }
    });
    
    console.log(`💰 Applied currency format to ${sheetName} columns: ${columns.join(', ')}`);
  } catch (error) {
    console.error(`Currency format error for ${sheetName}:`, error.message);
  }
}

// Apply Calibri 11 font to entire sheet
async function applyStandardFont(sheets, sheetName) {
  try {
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId: SPREADSHEET_ID
    });
    const sheet = spreadsheet.data.sheets.find(s => s.properties.title === sheetName);
    
    if (!sheet) {
      console.log(`Sheet "${sheetName}" not found for font formatting`);
      return;
    }
    
    const sheetId = sheet.properties.sheetId;
    
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{
          repeatCell: {
            range: {
              sheetId: sheetId
            },
            cell: {
              userEnteredFormat: {
                textFormat: {
                  fontFamily: 'Calibri',
                  fontSize: 11
                }
              }
            },
            fields: 'userEnteredFormat.textFormat(fontFamily,fontSize)'
          }
        }]
      }
    });
    
    console.log(`🔤 Applied Calibri 11 font to ${sheetName}`);
  } catch (error) {
    console.error(`Font format error for ${sheetName}:`, error.message);
  }
}

// Format all money columns across all sheets
async function formatAllSheetsCurrency() {
  if (!userTokens) {
    console.log('⚠️ Cannot format sheets - Google not connected');
    return;
  }
  
  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    // Get all sheet names
    const sheetNames = ['Wholesale Pricing', 'Bank Transactions', 'Invoices', 'Retail Sales', 'Inventory'];
    
    // Apply Calibri 11 font to all sheets
    for (const sheetName of sheetNames) {
      await applyStandardFont(sheets, sheetName);
    }
    
    // Wholesale Pricing: Column D (index 3) and Column H (index 7) have prices
    await applyCurrencyFormat(sheets, 'Wholesale Pricing', [3, 7], 3);
    
    // Bank Transactions: Column E (Debit, index 4) and Column F (Credit, index 5)
    await applyCurrencyFormat(sheets, 'Bank Transactions', [4, 5], 3);
    
    // Invoices: Column D (Total, index 3)
    await applyCurrencyFormat(sheets, 'Invoices', [3], 3);
    
    // Retail Sales: Need to find columns dynamically based on header
    try {
      const retailResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: 'Retail Sales!A2:Z2'
      });
      const headerRow = retailResponse.data.values?.[0] || [];
      
      // Find product columns and total/fee/net columns
      let productStartColIndex = -1;
      let totalColIndex = -1;
      
      for (let i = 0; i < headerRow.length; i++) {
        const header = headerRow[i];
        if (header === 'Total Retail Sales') {
          totalColIndex = i;
          break;
        }
        if (header && header !== 'Date' && header !== '' && productStartColIndex === -1) {
          productStartColIndex = i;
        }
      }
      
      if (productStartColIndex > -1 && totalColIndex > -1) {
        // Build array of all money columns (products through net payout)
        const moneyColumns = [];
        for (let i = productStartColIndex; i <= totalColIndex + 2; i++) {
          moneyColumns.push(i);
        }
        await applyCurrencyFormat(sheets, 'Retail Sales', moneyColumns, 3);
      }
    } catch (e) {
      console.log('Retail Sales sheet not found or error:', e.message);
    }
    
    console.log('✅ All sheets formatted with Calibri 11 and currency');
  } catch (error) {
    console.error('Format all sheets error:', error.message);
  }
}

// ============ Inventory Sync with Google Sheets ============

// Sync inventory TO Google Sheets - Single "Inventory" sheet format
async function syncInventoryToSheets() {
  if (!userTokens) {
    console.log('⚠️ Cannot sync inventory - Google not connected');
    return { success: false, error: 'Google not connected' };
  }

  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    // Generate PST timestamp
    const now = new Date();
    const pstTimestamp = now.toLocaleString('en-US', {
      timeZone: 'America/Los_Angeles',
      month: 'long',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    }) + ' PST';

    // Build combined inventory data (Column A empty, Row 1 empty)
    const data = [];
    
    // Row 1: empty
    data.push(['', '', '', '', '', '', '', '']);
    
    // Row 2: Last updated timestamp
    data.push(['', `Last updated: ${pstTimestamp}`, '', '', '', '', '', '']);
    
    // Row 3: empty (spacing)
    data.push(['', '', '', '', '', '', '', '']);
    
    // GREEN COFFEE SECTION
    data.push(['', 'Green Coffee Inventory', '', '', '', '', '', '']);
    data.push(['', 'Name', 'Weight (lb)', 'Roast Profile', 'Drop Temp', '', '', '']);
    greenCoffeeInventory.forEach(c => {
      data.push(['', c.name, c.weight, c.roastProfile || '', c.dropTemp || '', '', '', '']);
    });
    
    // Empty row
    data.push(['', '', '', '', '', '', '', '']);
    
    // ROASTED COFFEE SECTION
    data.push(['', 'Roasted Coffee Inventory', '', '', '', '', '', '']);
    data.push(['', 'Name', 'Weight (lb)', 'Type', 'Recipe', '', '', '']);
    roastedCoffeeInventory.forEach(c => {
      const recipe = c.recipe ? c.recipe.map(r => `${r.percentage}% ${r.name}`).join(' + ') : 'N/A';
      data.push(['', c.name, c.weight, c.type || '', recipe, '', '', '']);
    });
    
    // Empty row
    data.push(['', '', '', '', '', '', '', '']);
    
    // EN ROUTE SECTION
    // En Route: ID in column B (internal), then Name, Weight, Tracking, Date Ordered, Est. Delivery
    data.push(['', 'En Route Inventory', '', '', '', '', '']);
    data.push(['', 'ID', 'Name', 'Weight (lb)', 'Tracking Number', 'Date Ordered', 'Est. Delivery']);
    if (enRouteCoffeeInventory.length > 0) {
      enRouteCoffeeInventory.forEach(c => {
        // Only show estimated delivery if tracking number exists
        const estDelivery = c.trackingNumber ? (c.estimatedDelivery || '') : '';
        
        // Format dateOrdered as mm/dd/yy
        let dateOrdered = c.dateOrdered || c.orderDate || c.dateAdded || '';
        if (dateOrdered && !dateOrdered.match(/^\d{2}\/\d{2}\/\d{2}$/)) {
          // Try to convert to mm/dd/yy format
          try {
            const d = new Date(dateOrdered);
            if (!isNaN(d.getTime())) {
              const month = String(d.getMonth() + 1).padStart(2, '0');
              const day = String(d.getDate()).padStart(2, '0');
              const year = String(d.getFullYear()).slice(-2);
              dateOrdered = `${month}/${day}/${year}`;
            }
          } catch (e) {}
        }
        
        // ID in column B, then Name, Weight, Tracking, Date, Est. Delivery
        data.push(['', c.id, c.name, c.weight, c.trackingNumber || '', dateOrdered, estDelivery]);
      });
    }

    // Clear and write to Inventory sheet
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Inventory!A:G'
    }).catch(() => {}); // Ignore if sheet doesn't exist

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Inventory!A1',
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: data }
    }).catch(async (err) => {
      // Sheet might not exist, try to create it
      console.log('Creating Inventory sheet...');
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: {
          requests: [{ addSheet: { properties: { title: 'Inventory' } } }]
        }
      });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: 'Inventory!A1',
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: data }
      });
    });

    const timestamp = new Date().toISOString();
    console.log(`✅ Inventory synced to Google Sheets at ${timestamp}`);
    return { success: true, timestamp };

  } catch (error) {
    console.error('❌ Inventory sync error:', error.message);
    return { success: false, error: error.message };
  }
}

// Load inventory FROM Google Sheets - reads from single "Inventory" sheet
async function loadInventoryFromSheets() {
  if (!userTokens) {
    console.log('⚠️ Cannot load inventory - Google not connected, using defaults');
    return { success: false, error: 'Google not connected' };
  }

  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Inventory!A:H'  // Read extra columns to handle various formats
    });
    
    const rows = response.data.values || [];
    if (rows.length === 0) {
      console.log('⚠️ Inventory sheet empty, using defaults');
      return { success: false, error: 'Sheet empty' };
    }

    // Parse the combined sheet - find section headers
    let currentSection = null;
    const tempGreen = [];
    const tempRoasted = [];
    const tempEnRoute = [];

    // Helper to parse Recipe string like "66.6667% Brazil Mogiano + 33.3333% Ethiopia Yirgacheffe"
    const parseRecipeString = (recipeStr) => {
      if (!recipeStr || recipeStr === 'N/A' || recipeStr.toLowerCase() === 'n/a') {
        return null;
      }
      
      const recipe = [];
      // Split by " + " to get each component
      const parts = recipeStr.split(/\s*\+\s*/);
      
      for (const part of parts) {
        // Match pattern like "66.6667% Brazil Mogiano" or "100% Ethiopia Gera"
        const match = part.match(/^([\d.]+)%\s+(.+)$/);
        if (match) {
          const percentage = parseFloat(match[1]);
          const name = match[2].trim();
          const greenCoffeeId = name.toLowerCase().replace(/\s+/g, '-');
          recipe.push({
            greenCoffeeId,
            name,
            percentage
          });
        }
      }
      
      return recipe.length > 0 ? recipe : null;
    };

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const cellB = (row[1] || '').toString().toLowerCase();
      
      // Detect section headers
      if (cellB.includes('green coffee inventory')) {
        currentSection = 'green';
        continue;
      } else if (cellB.includes('roasted coffee inventory')) {
        currentSection = 'roasted';
        continue;
      } else if (cellB.includes('en route inventory')) {
        currentSection = 'enroute';
        continue;
      }
      
      // Skip header rows, empty rows, and timestamp row
      if (!row[1] || cellB === 'name' || cellB === '' || cellB.startsWith('last updated')) continue;
      
      // Parse data based on current section
      if (currentSection === 'green' && row[1]) {
        tempGreen.push({
          id: row[1].toLowerCase().replace(/\s+/g, '-'),
          name: row[1],
          weight: parseFloat(row[2]) || 0,
          roastProfile: String(row[3] || '').replace(/\.0$/, ''), // Remove trailing .0
          dropTemp: parseFloat(row[4]) || 0
        });
      } else if (currentSection === 'roasted' && row[1]) {
        const recipeStr = row[4] || '';
        const recipe = parseRecipeString(recipeStr);
        
        // Determine type based on recipe or column value
        let type = row[3] || '';
        if (!type && recipe) {
          type = recipe.length > 1 ? 'Blend' : 'Single Origin';
        }
        
        tempRoasted.push({
          id: row[1].toLowerCase().replace(/\s+/g, '-'),
          name: row[1],
          weight: parseFloat(row[2]) || 0,
          type: type,
          recipe: recipe
        });
      } else if (currentSection === 'enroute' && row[1]) {
        // Current format: B=ID, C=Name, D=Weight, E=Tracking, F=Date, G=Est
        // Also handle older formats for backwards compatibility
        
        const colB = String(row[1] || '').toLowerCase();
        
        // Skip header rows
        if (colB === 'name' || colB === 'id') continue;
        
        // Check if col B looks like an ID
        const colBLooksLikeId = colB.startsWith('enroute-');
        
        // Check for ID in column H (old hidden format)
        const idAtEnd = row[7] ? String(row[7]) : '';
        
        if (colBLooksLikeId) {
          // Current format: B=ID, C=Name, D=Weight, E=Tracking, F=Date, G=Est
          tempEnRoute.push({
            id: row[1],
            name: row[2] || '',
            weight: parseFloat(row[3]) || 0,
            type: '',
            trackingNumber: row[4] || '',
            dateOrdered: row[5] || '',
            estimatedDelivery: row[6] || ''
          });
        } else if (idAtEnd && idAtEnd.startsWith('enroute-')) {
          // Old format with ID hidden at end: B=Name, C=Weight, D=Tracking, E=Date, F=Est, G=empty, H=ID
          tempEnRoute.push({
            id: idAtEnd,
            name: row[1] || '',
            weight: parseFloat(row[2]) || 0,
            type: '',
            trackingNumber: row[3] || '',
            dateOrdered: row[4] || '',
            estimatedDelivery: row[5] || ''
          });
        } else if (String(row[0] || '').startsWith('enroute-')) {
          // Transitional format with ID in column A
          tempEnRoute.push({
            id: row[0],
            name: row[1] || '',
            weight: parseFloat(row[2]) || 0,
            type: '',
            trackingNumber: row[3] || '',
            dateOrdered: row[4] || '',
            estimatedDelivery: row[5] || ''
          });
        } else {
          // Oldest format without ID: B=Name, C=Weight, D=Tracking, E=Date, F=Est
          const name = row[1];
          tempEnRoute.push({
            id: `enroute-${name.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}`,
            name: name,
            weight: parseFloat(row[2]) || 0,
            type: '',
            trackingNumber: row[3] || '',
            dateOrdered: row[4] || '',
            estimatedDelivery: row[5] || ''
          });
        }
      }
    }

    // Only update if we found data
    if (tempGreen.length > 0) {
      greenCoffeeInventory = tempGreen;
      console.log(`📗 Loaded ${greenCoffeeInventory.length} green coffees from Sheets`);
    }
    if (tempRoasted.length > 0) {
      roastedCoffeeInventory = tempRoasted;
      console.log(`📕 Loaded ${roastedCoffeeInventory.length} roasted coffees from Sheets`);
    }
    // For en route, update even if empty (might have been cleared)
    enRouteCoffeeInventory = tempEnRoute;
    if (tempEnRoute.length > 0) {
      console.log(`📦 Loaded ${enRouteCoffeeInventory.length} en route items from Sheets`);
    }

    return { success: true };

  } catch (error) {
    console.error('❌ Load inventory error:', error.message);
    return { success: false, error: error.message };
  }
}

// ============ Google Sheets as Ground Truth ============
// Always fetch fresh data from Sheets before any inventory operation
async function ensureFreshInventory() {
  console.log('🔄 Fetching fresh inventory from Google Sheets...');
  const result = await loadInventoryFromSheets();
  if (!result.success) {
    console.log('⚠️ Using cached inventory - Sheets load failed:', result.error);
  }
  return result;
}

// Schedule weekly sync: Every Thursday at 11:59 PM Los Angeles time
let lastSyncTime = null;
let nextScheduledSync = null;

function getNextThursday1159PM() {
  // Get current time in Los Angeles
  const now = new Date();
  const laTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
  
  // Find next Thursday
  const dayOfWeek = laTime.getDay(); // 0 = Sunday, 4 = Thursday
  let daysUntilThursday = (4 - dayOfWeek + 7) % 7;
  
  // If it's Thursday, check if we're past 11:59 PM
  if (daysUntilThursday === 0) {
    const currentHour = laTime.getHours();
    const currentMinute = laTime.getMinutes();
    if (currentHour > 23 || (currentHour === 23 && currentMinute >= 59)) {
      daysUntilThursday = 7; // Next Thursday
    }
  }
  
  // If today is past Thursday, wait until next week
  if (daysUntilThursday === 0 && laTime.getHours() >= 23 && laTime.getMinutes() >= 59) {
    daysUntilThursday = 7;
  }
  
  // Calculate target date in LA timezone
  const targetLA = new Date(laTime);
  targetLA.setDate(targetLA.getDate() + daysUntilThursday);
  targetLA.setHours(23, 59, 0, 0);
  
  // Convert back to UTC for scheduling
  // Create a date string in LA timezone and parse it
  const targetString = targetLA.toLocaleString('en-US', { 
    timeZone: 'America/Los_Angeles',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
  
  // Parse and get the UTC equivalent
  const [datePart, timePart] = targetString.split(', ');
  const [month, day, year] = datePart.split('/');
  const [hour, minute, second] = timePart.split(':');
  
  // Create date in LA timezone, then get UTC time
  const laDate = new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}`);
  
  // Adjust for LA timezone offset (PST = -8, PDT = -7)
  // We need to add the offset to get UTC
  const janOffset = new Date(laDate.getFullYear(), 0, 1).getTimezoneOffset();
  const julOffset = new Date(laDate.getFullYear(), 6, 1).getTimezoneOffset();
  const isDST = laDate.getTimezoneOffset() < Math.max(janOffset, julOffset);
  const laOffsetHours = isDST ? 7 : 8; // PDT = -7, PST = -8
  
  const utcTarget = new Date(laDate.getTime() + (laOffsetHours * 60 * 60 * 1000));
  
  return utcTarget;
}

function scheduleWeeklySync() {
  const nextSync = getNextThursday1159PM();
  const now = new Date();
  const msUntilSync = nextSync.getTime() - now.getTime();
  
  // Ensure we don't schedule in the past
  const actualMs = Math.max(msUntilSync, 60000); // At least 1 minute
  
  nextScheduledSync = nextSync.toISOString();
  
  console.log(`📅 Next inventory sync scheduled for: ${nextSync.toLocaleString('en-US', { 
    timeZone: 'America/Los_Angeles',
    weekday: 'long',
    year: 'numeric', 
    month: 'long', 
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short'
  })}`);
  console.log(`   (in ${Math.round(actualMs / 1000 / 60 / 60)} hours)`);
  
  setTimeout(async () => {
    console.log('🔄 Running weekly inventory sync (Thursday 11:59 PM LA)...');
    const result = await syncInventoryToSheets();
    if (result.success) {
      lastSyncTime = result.timestamp;
    }
    // Schedule next week's sync
    scheduleWeeklySync();
  }, actualMs);
}

// Start the weekly schedule
scheduleWeeklySync();

// Also load from Sheets on startup after a delay (to allow Google connection)
setTimeout(async () => {
  console.log('🔄 Running startup inventory load...');
  await loadInventoryFromSheets();
}, 5000);

// ============ Google Status and Disconnect ============

app.get('/api/google/status', async (req, res) => {
  if (!userTokens) {
    return res.json({ connected: false, services: [] });
  }
  
  // Verify tokens are actually valid by trying to get access token
  try {
    oauth2Client.setCredentials(userTokens);
    const { token } = await oauth2Client.getAccessToken();
    if (token) {
      res.json({ 
        connected: true,
        services: ['Gmail', 'Sheets']
      });
    } else {
      console.log('⚠️ Google token refresh failed, clearing tokens');
      userTokens = null;
      res.json({ connected: false, services: [] });
    }
  } catch (error) {
    console.log('⚠️ Google token validation failed:', error.message);
    userTokens = null;
    res.json({ connected: false, services: [] });
  }
});

app.get('/api/google/disconnect', (req, res) => {
  userTokens = null;
  oauth2Client.revokeCredentials();
  req.session.destroy();
  res.json({ success: true, redirect: '/login' });
});

// ============ Inventory Sync Endpoints ============

// Manual sync to Google Sheets
app.post('/api/inventory/sync', async (req, res) => {
  console.log('📤 Manual inventory sync triggered');
  const result = await syncInventoryToSheets();
  if (result.success) {
    lastSyncTime = result.timestamp;
  }
  res.json(result);
});

// Load inventory from Google Sheets
app.post('/api/inventory/load', async (req, res) => {
  console.log('📥 Manual inventory load triggered');
  const result = await loadInventoryFromSheets();
  res.json(result);
});

// Get sync status
app.get('/api/inventory/sync-status', (req, res) => {
  res.json({
    lastSync: lastSyncTime,
    nextSync: nextScheduledSync,
    schedule: 'Every Thursday at 11:59 PM Los Angeles time',
    googleConnected: !!userTokens
  });
});

// ============ Gmail Routes ============

app.post('/api/gmail/send', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  try {
    const { to, subject, body, attachmentPath, attachmentName } = req.body;
    
    if (!to || !subject || !body) {
      return res.status(400).json({ error: 'Missing required fields: to, subject, body' });
    }
    
    oauth2Client.setCredentials(userTokens);
    const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
    
    let emailContent;
    
    if (attachmentPath && fs.existsSync(attachmentPath)) {
      // Email with PDF attachment
      const boundary = 'boundary_' + Date.now();
      const pdfData = fs.readFileSync(attachmentPath);
      const pdfBase64 = pdfData.toString('base64');
      const filename = attachmentName || path.basename(attachmentPath);
      
      emailContent = [
        `To: ${to}`,
        `Subject: ${subject}`,
        'MIME-Version: 1.0',
        `Content-Type: multipart/mixed; boundary="${boundary}"`,
        '',
        `--${boundary}`,
        'Content-Type: text/plain; charset=utf-8',
        '',
        body,
        '',
        `--${boundary}`,
        `Content-Type: application/pdf; name="${filename}"`,
        'Content-Transfer-Encoding: base64',
        `Content-Disposition: attachment; filename="${filename}"`,
        '',
        pdfBase64,
        '',
        `--${boundary}--`
      ].join('\r\n');
      
      console.log(`📎 Attaching PDF: ${filename}`);
    } else {
      // Plain text email
      emailContent = [
        `To: ${to}`,
        `Subject: ${subject}`,
        'Content-Type: text/plain; charset=utf-8',
        '',
        body
      ].join('\n');
    }
    
    const encodedEmail = Buffer.from(emailContent)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    
    // Create draft instead of sending
    const draft = await gmail.users.drafts.create({
      userId: 'me',
      requestBody: {
        message: { raw: encodedEmail }
      }
    });
    
    console.log(`📝 Draft created for ${to}${attachmentPath ? ' with attachment' : ''}`);
    res.json({ 
      success: true, 
      message: `Draft created! Check your Gmail drafts folder. What else can I help you with?`,
      draftId: draft.data.id
    });
    
  } catch (error) {
    console.error('Email draft error:', error);
    res.status(500).json({ error: 'Failed to create draft', details: error.message });
  }
});

app.get('/api/gmail/recent', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  try {
    oauth2Client.setCredentials(userTokens);
    const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
    
    const response = await gmail.users.messages.list({
      userId: 'me',
      maxResults: 10,
      q: req.query.q || ''
    });
    
    const messages = [];
    for (const msg of response.data.messages || []) {
      const detail = await gmail.users.messages.get({
        userId: 'me',
        id: msg.id,
        format: 'metadata',
        metadataHeaders: ['From', 'Subject', 'Date']
      });
      
      const headers = detail.data.payload.headers;
      messages.push({
        id: msg.id,
        from: headers.find(h => h.name === 'From')?.value,
        subject: headers.find(h => h.name === 'Subject')?.value,
        date: headers.find(h => h.name === 'Date')?.value,
        snippet: detail.data.snippet
      });
    }
    
    res.json({ messages });
    
  } catch (error) {
    console.error('Gmail read error:', error);
    res.status(500).json({ error: 'Failed to read emails', details: error.message });
  }
});

// ============ Google Sheets Routes ============

app.get('/api/sheets/read', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  try {
    const { spreadsheetId, range } = req.query;
    
    if (!spreadsheetId || !range) {
      return res.status(400).json({ error: 'Missing spreadsheetId or range' });
    }
    
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range
    });
    
    res.json({ values: response.data.values || [] });
    
  } catch (error) {
    console.error('Sheets read error:', error);
    res.status(500).json({ error: 'Failed to read spreadsheet', details: error.message });
  }
});

app.post('/api/sheets/write', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  try {
    const { spreadsheetId, range, values } = req.body;
    
    if (!spreadsheetId || !range || !values) {
      return res.status(400).json({ error: 'Missing spreadsheetId, range, or values' });
    }
    
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    const response = await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values }
    });
    
    console.log(`✅ Spreadsheet updated: ${range}`);
    res.json({ success: true, updatedCells: response.data.updatedCells });
    
  } catch (error) {
    console.error('Sheets write error:', error);
    res.status(500).json({ error: 'Failed to write to spreadsheet', details: error.message });
  }
});

app.post('/api/sheets/append', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  try {
    const { spreadsheetId, range, values } = req.body;
    
    if (!spreadsheetId || !range || !values) {
      return res.status(400).json({ error: 'Missing spreadsheetId, range, or values' });
    }
    
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    const response = await sheets.spreadsheets.values.append({
      spreadsheetId,
      range,
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values }
    });
    
    console.log(`✅ Row appended to spreadsheet`);
    res.json({ success: true, updatedRange: response.data.updates.updatedRange });
    
  } catch (error) {
    console.error('Sheets append error:', error);
    res.status(500).json({ error: 'Failed to append to spreadsheet', details: error.message });
  }
});

// Get list of known customers
app.get('/api/customers', (req, res) => {
  res.json({ customers: getKnownCustomers() });
});

// Get customer info (including emails)
app.get('/api/customers/:name', (req, res) => {
  const customerName = req.params.name;
  const lower = customerName.toLowerCase();
  
  if (customerDirectory[lower]) {
    res.json(customerDirectory[lower]);
  } else {
    res.status(404).json({ error: 'Customer not found' });
  }
});

// Add a new customer
app.post('/api/customers/add', (req, res) => {
  const { name, code, emails } = req.body;
  
  if (!name) {
    return res.status(400).json({ error: 'Customer name required' });
  }
  
  if (!code || code.length !== 3) {
    return res.status(400).json({ error: 'Three letter code required' });
  }
  
  const trimmedName = name.trim();
  const trimmedCode = code.trim().toUpperCase();
  const lower = trimmedName.toLowerCase();
  
  if (customerDirectory[lower]) {
    return res.status(400).json({ error: `Customer "${trimmedName}" already exists` });
  }
  
  addOrUpdateCustomer(trimmedName, trimmedCode, emails || []);
  console.log(`✅ Added new customer: ${trimmedName} (code: ${trimmedCode})`);
  
  res.json({ 
    success: true, 
    message: `"${trimmedName}" (${trimmedCode}) has been added as a new customer. They will receive Wholesale Tier 1 pricing.`,
    customers: getKnownCustomers()
  });
});

// Update customer emails
app.post('/api/customers/emails', async (req, res) => {
  const { customerName, emailsInput } = req.body;
  
  if (!customerName || !emailsInput) {
    return res.status(400).json({ error: 'Customer name and emails required' });
  }
  
  // Use Gemini to parse the emails from natural language
  const parsePrompt = `Extract all email addresses from this input: "${emailsInput}"
  
Respond ONLY with valid JSON (no markdown):
{
  "emails": ["email1@example.com", "email2@example.com"]
}

If no valid emails found, return: {"emails": []}`;

  try {
    const parseText = await callGeminiWithRetry(parsePrompt, { maxRetries: 2 });
    const cleanJson = parseText.replace(/```json\n?|\n?```/g, '').trim();
    const parsed = JSON.parse(cleanJson);
    
    if (!parsed.emails || parsed.emails.length === 0) {
      return res.status(400).json({ error: 'No valid email addresses found' });
    }
    
    const lower = customerName.toLowerCase();
    if (customerDirectory[lower]) {
      customerDirectory[lower].emails = parsed.emails;
      console.log(`✅ Updated emails for ${customerName}: ${parsed.emails.join(', ')}`);
      res.json({ success: true, emails: parsed.emails });
    } else {
      return res.status(404).json({ error: 'Customer not found' });
    }
  } catch (error) {
    console.error('Email parsing error:', error);
    // Fallback: try simple regex extraction
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
    const emails = emailsInput.match(emailRegex) || [];
    
    if (emails.length === 0) {
      return res.status(400).json({ error: 'No valid email addresses found' });
    }
    
    const lower = customerName.toLowerCase();
    if (customerDirectory[lower]) {
      customerDirectory[lower].emails = emails;
      console.log(`✅ Updated emails for ${customerName}: ${emails.join(', ')}`);
      res.json({ success: true, emails: emails });
    } else {
      return res.status(404).json({ error: 'Customer not found' });
    }
  }
});

// Interpret yes/no confirmation using Gemini
app.post('/api/interpret-confirmation', async (req, res) => {
  const { message } = req.body;
  
  if (!message) {
    return res.status(400).json({ error: 'Message required' });
  }
  
  const prompt = `Interpret if the user is saying YES or NO to a question.
User's response: "${message}"

Respond ONLY with valid JSON (no markdown):
{"confirmed": true} if the user is agreeing/confirming (e.g., yes, sure, ok, yeah, yep, absolutely, go ahead, do it, sounds good, please, definitely)
{"confirmed": false} if the user is declining/canceling (e.g., no, nope, cancel, never mind, don't, nah, skip)
{"confirmed": null} if unclear`;

  try {
    const responseText = await callGeminiWithRetry(prompt, { maxRetries: 1 });
    const cleanJson = responseText.replace(/```json\n?|\n?```/g, '').trim();
    const parsed = JSON.parse(cleanJson);
    res.json(parsed);
  } catch (error) {
    console.error('Error interpreting confirmation:', error);
    // Fallback
    const lower = message.toLowerCase();
    if (lower.includes('yes') || lower.includes('sure') || lower.includes('ok') || lower.includes('yep') || lower.includes('yeah')) {
      res.json({ confirmed: true });
    } else if (lower.includes('no') || lower.includes('nope') || lower.includes('cancel')) {
      res.json({ confirmed: false });
    } else {
      res.json({ confirmed: null });
    }
  }
});

// ============ Invoice Generation ============

app.post('/api/invoice/generate', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected. Please connect Google first.' });
  }

  try {
    const { details, confirmNewCustomer, newCustomerName, newCustomerCode } = req.body;
    
    // Handle adding a new customer
    if (confirmNewCustomer && newCustomerName) {
      const lower = newCustomerName.toLowerCase();
      if (!customerDirectory[lower]) {
        const code = newCustomerCode || newCustomerName.substring(0, 3).toUpperCase();
        addOrUpdateCustomer(newCustomerName, code, []);
        console.log(`✅ Added new customer: ${newCustomerName} (${code})`);
      }
      // Continue processing with the new customer name
    }
    
    if (!details) {
      return res.status(400).json({ error: 'Invoice details required' });
    }

    // Use Gemini to parse multiple items from natural language
    const knownProducts = ['Archives Blend', 'Ethiopia Gera Natural', 'Colombia Excelso', 'Colombia Decaf'];
    const productAliases = {
      'archives': 'Archives Blend', 'archive': 'Archives Blend', 'house': 'Archives Blend', 'blend': 'Archives Blend',
      'ethiopia': 'Ethiopia Gera Natural', 'ethiopian': 'Ethiopia Gera Natural', 'ethiopia gera': 'Ethiopia Gera Natural',
      'colombia': 'Colombia Excelso', 'colombian': 'Colombia Excelso',
      'decaf': 'Colombia Decaf', 'decaffeinated': 'Colombia Decaf'
    };
    
    const parsePrompt = `Parse this invoice request into customer and line items.
Input: "${details}"

KNOWN CUSTOMERS: ${getKnownCustomers().join(', ')}
KNOWN PRODUCTS: Archives Blend, Ethiopia Gera Natural, Colombia Excelso, Colombia Decaf
PRODUCT ALIASES: archives/archive/blend = "Archives Blend", ethiopia/ethiopian = "Ethiopia Gera Natural", colombia/colombian = "Colombia Excelso", decaf = "Colombia Decaf"

Respond ONLY with valid JSON (no markdown):
{
  "customer": "customer name from known customers list",
  "items": [
    {"quantity": 100, "product": "exact product name from known products"},
    {"quantity": 40, "product": "exact product name from known products"}
  ]
}

Examples:
- "AOU 100lb Archives blend and 40lb Ethiopia" → {"customer": "Archives of Us", "items": [{"quantity": 100, "product": "Archives Blend"}, {"quantity": 40, "product": "Ethiopia Gera Natural"}]}
- "CED 50 lbs archives" → {"customer": "CED", "items": [{"quantity": 50, "product": "Archives Blend"}]}
- "Dex 20lb ethiopia, 30lb decaf" → {"customer": "Dex", "items": [{"quantity": 20, "product": "Ethiopia Gera Natural"}, {"quantity": 30, "product": "Colombia Decaf"}]}`;

    let customer = null;
    let items = [];
    
    try {
      const parseText = await callGeminiWithRetry(parsePrompt, { maxRetries: 2 });
      console.log(`🤖 Gemini parse: ${parseText}`);
      
      const cleanJson = parseText.replace(/```json\n?|\n?```/g, '').trim();
      const parsed = JSON.parse(cleanJson);
      
      customer = parsed.customer;
      items = parsed.items || [];
    } catch (parseError) {
      console.error('⚠️ Gemini parsing failed:', parseError.message);
      return res.json({ 
        success: false,
        error: "Sorry, I didn't get that. What can I help you with?",
        showFollowUp: false,
        action: 'unclear'
      });
    }
    
    if (!customer || !items || items.length === 0) {
      return res.json({ 
        success: false,
        error: "Sorry, I didn't get that. What can I help you with?",
        showFollowUp: false,
        action: 'unclear'
      });
    }
    
    // Match customer to known list
    const normalizedCustomer = getKnownCustomers().find(c => 
      c.toLowerCase() === customer.toLowerCase() ||
      customer.toLowerCase().includes(c.toLowerCase()) ||
      c.toLowerCase().includes(customer.toLowerCase())
    );
    const finalCustomer = normalizedCustomer || customer;

    console.log(`📝 Generating invoice for: ${finalCustomer}, ${items.length} item(s)`);

    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    // Step 1: Get pricing from Wholesale Pricing sheet (entire sheet including At-Cost)
    const pricingResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Wholesale Pricing!A:H'  // Extended to column H for At-Cost pricing
    });
    
    const pricingRows = pricingResponse.data.values || [];
    
    // Use Gemini to find the correct pricing for each item
    const pricingPrompt = `You are a pricing lookup assistant. Find the correct prices from this spreadsheet.

SPREADSHEET DATA (Wholesale Pricing sheet):
${pricingRows.map((row, i) => `Row ${i + 1}: ${row.map((cell, j) => `${String.fromCharCode(65 + j)}="${cell || ''}"`).join(', ')}`).join('\n')}

PRICING RULES:
1. For customer "Archives of Us" or "AOU": Use the "At-Cost" table and get price from column H
2. For customer "CED": Use "Wholesale CED" table and get price from column D
3. For customer "Dex": Use "Wholesale Dex" table and get price from column D
4. For customer "Junia": Use "Wholesale Junia" table and get price from column D
5. For any other customer: Use "Wholesale Tier 1" table and get price from column D

LOOKUP REQUEST:
- Customer: "${finalCustomer}"
- Items to price: ${items.map(item => `"${item.product}"`).join(', ')}

Find the per-lb price for each product from the correct table for this customer.

Respond ONLY with valid JSON (no markdown):
{
  "table": "<name of table used>",
  "prices": {
    "<product name as given>": <price as number>,
    "<product name as given>": <price as number>
  }
}`;

    let pricingMap = {};
    let pricingSource = 'unknown';
    
    try {
      const pricingText = await callGeminiWithRetry(pricingPrompt, { maxRetries: 2 });
      console.log(`🤖 Gemini pricing response: ${pricingText}`);
      
      const cleanJson = pricingText.replace(/```json\n?|\n?```/g, '').trim();
      const pricingData = JSON.parse(cleanJson);
      
      pricingSource = pricingData.table;
      
      // Build pricing map from Gemini response
      for (const [product, price] of Object.entries(pricingData.prices)) {
        if (price && price > 0) {
          pricingMap[product.toLowerCase()] = { name: product, price: parseFloat(price) };
        }
      }
      
      console.log(`✅ Gemini found prices from ${pricingSource}:`, pricingMap);
    } catch (error) {
      console.log('⚠️ Gemini pricing failed, using direct lookup:', error.message);
      
      // Fallback: Direct sheet parsing
      const isArchives = finalCustomer.toLowerCase() === 'archives of us';
      let targetTable = isArchives ? 'at-cost' : `wholesale ${finalCustomer.toLowerCase()}`;
      let priceColumn = isArchives ? 7 : 3; // H for At-Cost, D for Wholesale
      
      // Find the correct table start row
      let tableStartRow = -1;
      for (let i = 0; i < pricingRows.length; i++) {
        const cellB = (pricingRows[i][1] || '').toString().toLowerCase();
        if (cellB.includes(targetTable) || (isArchives && cellB === 'at-cost')) {
          tableStartRow = i;
          pricingSource = cellB;
          break;
        }
      }
      
      // Fallback to Tier 1 if no specific table found
      if (tableStartRow === -1 && !isArchives) {
        for (let i = 0; i < pricingRows.length; i++) {
          const cellB = (pricingRows[i][1] || '').toString().toLowerCase();
          if (cellB.includes('wholesale tier 1')) {
            tableStartRow = i;
            pricingSource = 'Wholesale Tier 1 (fallback)';
            break;
          }
        }
      }
      
      // Build pricing map from the table
      if (tableStartRow !== -1) {
        for (let i = tableStartRow + 1; i < pricingRows.length; i++) {
          const row = pricingRows[i];
          const productName = (row[1] || '').toString().trim();
          const priceCell = row[priceColumn];
          
          // Stop if we hit another table or empty row
          if (!productName || productName.toLowerCase().includes('wholesale') || productName.toLowerCase() === 'at-cost') break;
          if (productName.toLowerCase() === 'coffee') continue; // Skip header
          
          const price = parseFloat((priceCell || '').toString().replace(/[$,]/g, ''));
          if (price > 0) {
            pricingMap[productName.toLowerCase()] = { name: productName, price };
          }
        }
      }
    }
    
    console.log(`📋 Final pricing map (from ${pricingSource}):`, pricingMap);
    
    // Process each item and get pricing
    const processedItems = [];
    for (const item of items) {
      const productLower = item.product.toLowerCase();
      const pricing = pricingMap[productLower];
      
      if (!pricing) {
        return res.status(400).json({ 
          error: `Could not find pricing for "${item.product}" for customer "${finalCustomer}". Please check the pricing sheet.`
        });
      }
      
      processedItems.push({
        description: `${pricing.name} (units in lbs)`,
        quantity: item.quantity,
        unitPrice: pricing.price,
        total: item.quantity * pricing.price
      });
      
      console.log(`💰 ${item.quantity} lbs ${pricing.name}: $${pricing.price}/lb = $${(item.quantity * pricing.price).toFixed(2)}`);
    }
    
    // Calculate grand total
    const subtotal = processedItems.reduce((sum, item) => sum + item.total, 0);
    const grandTotal = subtotal;

    // Step 2: Get last invoice number from Invoices sheet
    const invoicesResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!A:E'
    });
    
    const invoiceRows = invoicesResponse.data.values || [];
    const customerPrefix = getCustomerCode(finalCustomer);
    
    let lastNumber = 999;
    // Direct search in column C for invoice numbers
    for (const row of invoiceRows) {
      if (row[2] && row[2].startsWith(`C-${customerPrefix}-`)) {
        const num = parseInt(row[2].split('-')[2]);
        if (!isNaN(num) && num > lastNumber) {
          lastNumber = num;
        }
      }
    }
    console.log(`✅ Last invoice for ${customerPrefix}: ${lastNumber}`);
    
    const invoiceNumber = `C-${customerPrefix}-${lastNumber + 1}`;
    console.log(`🧾 Generated invoice number: ${invoiceNumber}`);

    // Step 3: Generate dates
    const today = new Date();
    const dateStr = today.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });
    const dueDateObj = new Date(today);
    dueDateObj.setDate(dueDateObj.getDate() + 14); // Due in 2 weeks
    const dueDateStr = dueDateObj.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });

    // Step 4: Generate PDF with all items
    const pdfFilename = `${invoiceNumber}.pdf`;
    const pdfPath = path.join(invoicesDir, pdfFilename);
    
    await generateInvoicePDF({
      invoiceNumber,
      customer: finalCustomer,
      date: dateStr,
      dueDate: dueDateStr,
      items: processedItems,
      subtotal: subtotal,
      total: grandTotal
    }, pdfPath);

    console.log(`📄 PDF generated: ${pdfPath}`);

    // Don't record to spreadsheet yet - wait for confirmation

    // Return response with pending status
    res.json({
      success: true,
      pending: true,
      invoiceNumber,
      customer: finalCustomer,
      date: dateStr,
      dueDate: dueDateStr,
      items: processedItems,
      total: grandTotal,
      pdfUrl: `/invoices/${pdfFilename}`
    });

  } catch (error) {
    console.error('Invoice generation error:', error);
    res.status(500).json({ error: 'Failed to generate invoice', details: error.message });
  }
});

// Confirm invoice and record to spreadsheet
app.post('/api/invoice/confirm', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }

  // Always fetch fresh inventory from Google Sheets before modifying
  await ensureFreshInventory();

  try {
    const { invoiceNumber, date, total, items } = req.body;
    
    if (!invoiceNumber || !date || total === undefined) {
      return res.status(400).json({ error: 'Missing invoice details' });
    }

    // VALIDATION: Check if we have enough roasted coffee BEFORE making any changes
    const shortages = [];
    
    if (items && items.length > 0) {
      for (const item of items) {
        const productName = item.description ? item.description.replace(' (units in lbs)', '') : item.product;
        const quantity = item.quantity || 0;
        
        // Search for matching roasted coffee with flexible matching
        const roastedMatch = roastedCoffeeInventory.find(c => {
          const cName = c.name.toLowerCase();
          const pName = productName.toLowerCase();
          if (cName === pName) return true;
          if (cName.includes(pName) || pName.includes(cName)) return true;
          if (pName.includes('archives') && cName.includes('archives')) return true;
          if (pName.includes('ethiopia') && cName.includes('ethiopia')) return true;
          if (pName.includes('decaf') && cName.includes('decaf')) return true;
          return false;
        });
        
        if (roastedMatch && quantity > 0) {
          if (quantity > roastedMatch.weight) {
            shortages.push({
              name: roastedMatch.name,
              required: quantity,
              available: roastedMatch.weight,
              shortage: quantity - roastedMatch.weight
            });
          }
        }
      }
    }
    
    // If there are shortages, reject the invoice
    if (shortages.length > 0) {
      const shortageList = shortages.map(s => 
        `${s.name}: need ${s.required}lb but only ${s.available}lb available (short ${s.shortage}lb)`
      ).join('; ');
      
      return res.json({
        success: false,
        error: 'insufficient_inventory',
        message: `Not enough roasted coffee to complete this invoice. ${shortageList}. Please reduce the quantity or choose a different product. What would you like to do?`,
        shortages
      });
    }

    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    // Record in Invoices sheet (store as number, format will display as currency)
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!B:E',
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: {
        values: [[date, invoiceNumber, parseFloat(total), '']]
      }
    });

    // Apply currency format to Total column (column D, index 3)
    await applyCurrencyFormat(sheets, 'Invoices', [3], 3);

    console.log(`✅ Invoice ${invoiceNumber} confirmed and recorded in spreadsheet`);

    // Deduct items from roasted coffee inventory
    const deductions = [];
    
    if (items && items.length > 0) {
      for (const item of items) {
        // Find matching roasted coffee by description/product name
        const productName = item.description ? item.description.replace(' (units in lbs)', '') : item.product;
        const quantity = item.quantity || 0;
        
        // Search for matching roasted coffee with flexible matching
        const roastedMatch = roastedCoffeeInventory.find(c => {
          const cName = c.name.toLowerCase();
          const pName = productName.toLowerCase();
          // Exact match
          if (cName === pName) return true;
          // Partial match (e.g., "Ethiopia Gera" matches "Ethiopia Gera Natural")
          if (cName.includes(pName) || pName.includes(cName)) return true;
          // Handle "Archives" matching "Archives Blend"
          if (pName.includes('archives') && cName.includes('archives')) return true;
          if (pName.includes('ethiopia') && cName.includes('ethiopia')) return true;
          if (pName.includes('decaf') && cName.includes('decaf')) return true;
          return false;
        });
        
        if (roastedMatch && quantity > 0) {
          const previousWeight = roastedMatch.weight;
          roastedMatch.weight -= quantity; // Already validated, won't go negative
          
          deductions.push({
            product: roastedMatch.name,
            deducted: quantity,
            previous: previousWeight,
            remaining: roastedMatch.weight
          });
          
          console.log(`📦 Deducted ${quantity} lb from ${roastedMatch.name}: ${previousWeight} → ${roastedMatch.weight} lb`);
        } else {
          console.log(`⚠️ No match found for "${productName}" in roasted inventory`);
        }
      }
    } else {
      console.log(`⚠️ No items array received for deduction`);
    }

    // Sync inventory to Sheets before responding
    await syncInventoryToSheets();

    res.json({ 
      success: true, 
      message: `Invoice ${invoiceNumber} confirmed and added to spreadsheet.${deductions.length > 0 ? ' Roasted inventory updated.' : ''} What else can I help you with?`,
      deductions: deductions
    });

  } catch (error) {
    console.error('Invoice confirmation error:', error);
    res.status(500).json({ error: 'Failed to confirm invoice', details: error.message });
  }
});

// ============ Invoice Payment Matching ============

// Match a payment against unpaid invoices
async function matchPaymentToInvoice(paymentAmount, paymentDate, paymentDescription) {
  if (!userTokens) {
    return { matched: false, reason: 'Google not connected' };
  }

  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    // Get all invoices from Invoices sheet
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!A:F'
    });

    const rows = response.data.values || [];
    if (rows.length < 2) {
      return { matched: false, reason: 'No invoices found' };
    }

    // Find unpaid invoices (column E or F might have "Paid" status)
    // Typical format: Date, Invoice#, Amount, Customer, Status, etc.
    const unpaidInvoices = [];
    
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 3) continue;
      
      // Try to extract invoice data - format varies
      // Common patterns: [Date, Invoice#, Amount] or [Empty, Date, Invoice#, Amount]
      let invoiceNum = '';
      let amount = 0;
      let date = '';
      let isPaid = false;
      
      for (let j = 0; j < row.length; j++) {
        const cell = (row[j] || '').toString();
        
        // Check for invoice number pattern (C-XXX-NNNN)
        if (cell.match(/^C-[A-Z]{2,4}-\d+$/)) {
          invoiceNum = cell;
        }
        // Check for dollar amount
        else if (cell.match(/^\$?[\d,]+\.?\d*$/)) {
          const parsed = parseFloat(cell.replace(/[$,]/g, ''));
          if (parsed > 0) amount = parsed;
        }
        // Check for date
        else if (cell.match(/^\d{1,2}\/\d{1,2}\/\d{2,4}$/)) {
          date = cell;
        }
        // Check for paid status
        else if (cell.toLowerCase() === 'paid' || cell.toLowerCase() === 'x') {
          isPaid = true;
        }
      }
      
      if (invoiceNum && amount > 0 && !isPaid) {
        unpaidInvoices.push({
          rowIndex: i + 1, // 1-indexed for Sheets
          invoiceNumber: invoiceNum,
          amount: amount,
          date: date,
          customerCode: invoiceNum.split('-')[1] || ''
        });
      }
    }

    if (unpaidInvoices.length === 0) {
      return { matched: false, reason: 'No unpaid invoices found' };
    }

    // Find matching invoice by amount (exact or within 1%)
    const tolerance = 0.01; // 1% tolerance
    const matches = unpaidInvoices.filter(inv => {
      const diff = Math.abs(inv.amount - paymentAmount) / inv.amount;
      return diff <= tolerance;
    });

    if (matches.length === 0) {
      return { matched: false, reason: 'No invoice matches this amount', unpaidInvoices };
    }

    if (matches.length === 1) {
      // Single match - high confidence
      const match = matches[0];
      
      // Get customer name from code
      const customerName = getCustomerNameFromCode(match.customerCode);
      
      return {
        matched: true,
        confidence: 'high',
        invoice: match,
        suggestedCustomer: customerName || match.customerCode,
        reason: `Exact amount match: $${paymentAmount} = Invoice ${match.invoiceNumber}`
      };
    }

    // Multiple matches - use Gemini to help disambiguate
    const prompt = `Help match a bank payment to an invoice.

Payment Details:
- Amount: $${paymentAmount}
- Date: ${paymentDate}
- Description: "${paymentDescription}"

Matching Invoices (same amount):
${matches.map(m => `- ${m.invoiceNumber}: $${m.amount}, dated ${m.date}, customer code ${m.customerCode}`).join('\n')}

Based on the payment date and description, which invoice is the best match?
Consider:
1. Date proximity (payment should be after invoice date)
2. Any customer name hints in the description

Respond with JSON only:
{
  "bestMatch": "invoice number or null if unclear",
  "confidence": "high/medium/low",
  "reason": "brief explanation"
}`;

    try {
      const geminiResponse = await callGeminiWithRetry(prompt, { temperature: 0.1 });
      const jsonMatch = geminiResponse.match(/\{[\s\S]*\}/);
      
      if (jsonMatch) {
        const result = JSON.parse(jsonMatch[0]);
        const matchedInvoice = matches.find(m => m.invoiceNumber === result.bestMatch);
        
        if (matchedInvoice) {
          const customerName = getCustomerNameFromCode(matchedInvoice.customerCode);
          return {
            matched: true,
            confidence: result.confidence,
            invoice: matchedInvoice,
            suggestedCustomer: customerName || matchedInvoice.customerCode,
            reason: result.reason
          };
        }
      }
    } catch (e) {
      console.error('Gemini matching error:', e);
    }

    // Fallback: return all matches for manual review
    return {
      matched: true,
      confidence: 'low',
      possibleMatches: matches,
      reason: 'Multiple invoices match this amount - manual review needed'
    };

  } catch (error) {
    console.error('Payment matching error:', error);
    return { matched: false, reason: error.message };
  }
}

// Get customer name from invoice code
function getCustomerNameFromCode(code) {
  // Search customerDirectory for matching code
  for (const [key, customer] of Object.entries(customerDirectory)) {
    if (customer.code === code) {
      return customer.name;
    }
  }
  return null;
}

// Mark invoice as paid in spreadsheet
async function markInvoicePaid(invoiceNumber, paymentDate, paymentMethod) {
  if (!userTokens) {
    return { success: false, error: 'Google not connected' };
  }

  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    // Find the invoice row
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!A:F'
    });

    const rows = response.data.values || [];
    let targetRow = -1;

    for (let i = 0; i < rows.length; i++) {
      if (rows[i].some(cell => cell === invoiceNumber)) {
        targetRow = i + 1;
        break;
      }
    }

    if (targetRow === -1) {
      return { success: false, error: 'Invoice not found' };
    }

    // Update the row to mark as paid (add to column E or next available)
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `Invoices!E${targetRow}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: [['Paid']]
      }
    });

    console.log(`✅ Marked invoice ${invoiceNumber} as paid`);
    return { success: true, invoiceNumber, paymentDate };

  } catch (error) {
    console.error('Mark paid error:', error);
    return { success: false, error: error.message };
  }
}

// API endpoint to match a payment
app.post('/api/payment/match', async (req, res) => {
  const { amount, date, description } = req.body;

  if (!amount) {
    return res.status(400).json({ error: 'Payment amount required' });
  }

  const result = await matchPaymentToInvoice(
    parseFloat(amount),
    date || new Date().toLocaleDateString(),
    description || ''
  );

  res.json(result);
});

// API endpoint to confirm a payment match and mark invoice paid
app.post('/api/payment/confirm', async (req, res) => {
  const { invoiceNumber, paymentDate, paymentMethod, customer } = req.body;

  if (!invoiceNumber) {
    return res.status(400).json({ error: 'Invoice number required' });
  }

  const result = await markInvoicePaid(invoiceNumber, paymentDate, paymentMethod);
  
  if (result.success) {
    // Also update Accounting sheet if it exists
    try {
      oauth2Client.setCredentials(userTokens);
      const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
      
      // Try to update the Income section with customer info
      // This is a best-effort update
      console.log(`💰 Payment confirmed for ${invoiceNumber} from ${customer || 'Unknown'}`);
    } catch (e) {
      // Non-critical error
    }
  }

  res.json(result);
});

// Auto-match payments when adding to accounting (called from frontend or Plaid)
app.post('/api/accounting/process-payment', async (req, res) => {
  const { amount, date, description, autoConfirm } = req.body;

  // Step 1: Try to match
  const matchResult = await matchPaymentToInvoice(
    parseFloat(amount),
    date,
    description
  );

  // Step 2: If high confidence match and autoConfirm, mark as paid
  if (matchResult.matched && matchResult.confidence === 'high' && autoConfirm) {
    const confirmResult = await markInvoicePaid(
      matchResult.invoice.invoiceNumber,
      date,
      'Auto-matched'
    );
    
    return res.json({
      ...matchResult,
      autoConfirmed: confirmResult.success,
      message: confirmResult.success 
        ? `Auto-matched and marked ${matchResult.invoice.invoiceNumber} as paid`
        : 'Matched but could not auto-confirm'
    });
  }

  // Step 3: Return match result for manual review
  res.json({
    ...matchResult,
    autoConfirmed: false,
    message: matchResult.matched 
      ? `Found match: ${matchResult.invoice?.invoiceNumber || 'multiple options'} (${matchResult.confidence} confidence)`
      : matchResult.reason
  });
});

// ============ Bank Transactions Sheet Sync ============

// Auto-categorize a transaction based on description
function categorizeTransaction(description) {
  const desc = description.toUpperCase();
  
  if (desc.includes('SHARED ROASTING')) return 'Roasting Fee';
  if (desc.includes('ROYAL COFFEE')) return 'Green Coffee';
  if (desc.includes('ACCURATE') || desc.includes('FREIGHT') || desc.includes('SHIPPING')) return 'Shipping';
  if (desc.includes('JPMORGAN') || desc.includes('CHASE') || desc.includes('FEE')) return 'Bank Fees';
  if (desc.includes('TRANSFER FROM')) return 'Internal Transfer';
  if (desc.includes('DEPOSIT') || desc.includes('PAYMENT')) return 'Customer Payment';
  
  // Check for known customer names
  for (const [key, customer] of Object.entries(customerDirectory)) {
    if (desc.includes(customer.name.toUpperCase())) return 'Customer Payment';
  }
  
  return 'Other';
}

// Add a transaction to Bank Transactions sheet
async function addBankTransaction(transaction) {
  if (!userTokens) {
    return { success: false, error: 'Google not connected' };
  }

  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    const { date, description, debit, credit, notes } = transaction;
    const category = transaction.category || categorizeTransaction(description);

    // Append to Bank Transactions sheet
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Bank Transactions!B:H',
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: {
        values: [[date, description, category, debit || '', credit || '', '', notes || '']]
      }
    });

    // Apply currency format to Debit (column E, index 4) and Credit (column F, index 5)
    await applyCurrencyFormat(sheets, 'Bank Transactions', [4, 5], 3);

    console.log(`📊 Added transaction: ${description} (${category})`);

    // If it's a customer payment, try to match to invoice
    if (category === 'Customer Payment' && credit > 0) {
      const matchResult = await matchPaymentToInvoice(credit, date, description);
      if (matchResult.matched && matchResult.confidence === 'high') {
        await markInvoicePaid(matchResult.invoice.invoiceNumber, date, 'Auto-matched');
        return { 
          success: true, 
          category, 
          matched: true, 
          invoice: matchResult.invoice.invoiceNumber 
        };
      }
    }

    return { success: true, category };

  } catch (error) {
    console.error('Add transaction error:', error);
    return { success: false, error: error.message };
  }
}

// API endpoint to add a bank transaction
app.post('/api/bank/transaction', async (req, res) => {
  const { date, description, debit, credit, notes } = req.body;

  if (!description) {
    return res.status(400).json({ error: 'Description required' });
  }

  const result = await addBankTransaction({
    date: date || new Date().toLocaleDateString(),
    description,
    debit: debit ? parseFloat(debit) : null,
    credit: credit ? parseFloat(credit) : null,
    notes
  });

  res.json(result);
});

// Bulk import transactions (for Plaid sync)
app.post('/api/bank/sync', async (req, res) => {
  const { transactions } = req.body;

  if (!transactions || !Array.isArray(transactions)) {
    return res.status(400).json({ error: 'Transactions array required' });
  }

  const results = [];
  for (const t of transactions) {
    const result = await addBankTransaction(t);
    results.push({ ...t, ...result });
  }

  const matched = results.filter(r => r.matched).length;
  const added = results.filter(r => r.success).length;

  res.json({
    success: true,
    total: transactions.length,
    added,
    matched,
    results
  });
});

// Update Bank Transactions sheet timestamp
async function updateBankTransactionsTimestamp() {
  if (!userTokens) return;

  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    const pstTimestamp = new Date().toLocaleString('en-US', {
      timeZone: 'America/Los_Angeles',
      month: 'long',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    }) + ' PST';

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Bank Transactions!B2',
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: [[`Last updated: ${pstTimestamp}`]]
      }
    });
  } catch (e) {
    // Non-critical
  }
}


// Parse invoice details from natural language
function parseInvoiceDetails(details) {
  // Pattern: "Customer, Quantity lbs Product" or "Customer, Quantity Product"
  const patterns = [
    /^([^,]+),?\s*(\d+)\s*(?:lbs?|pounds?)?\s+(.+)$/i,
    /^([^,]+),?\s*(\d+)\s+(.+)$/i
  ];
  
  for (const pattern of patterns) {
    const match = details.match(pattern);
    if (match) {
      return {
        customer: match[1].trim(),
        quantity: parseInt(match[2]),
        product: match[3].trim()
      };
    }
  }
  
  return null;
}

// Generate PDF invoice
async function generateInvoicePDF(data, outputPath) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ margin: 50 });
    const writeStream = fs.createWriteStream(outputPath);
    
    doc.pipe(writeStream);

    // Header
    doc.fontSize(20).font('Helvetica-Bold').text('Archives Of Us', { align: 'left' });
    doc.fontSize(10).font('Helvetica')
      .text('555 N Spring Suite 201')
      .text('Los Angeles, CA')
      .text('424.313.2013');

    // Invoice title
    doc.moveDown(2);
    doc.fontSize(24).font('Helvetica-Bold').text('Invoice', { align: 'right' });
    doc.fontSize(10).font('Helvetica').text(`Submitted on ${data.date}`, { align: 'right' });

    // Invoice details box
    doc.moveDown(2);
    const tableTop = doc.y;
    
    doc.fontSize(10).font('Helvetica-Bold');
    doc.text('Invoice for', 50, tableTop);
    doc.text('Payable to', 200, tableTop);
    doc.text('Invoice #', 350, tableTop);
    
    doc.font('Helvetica');
    doc.text(data.customer, 50, tableTop + 15);
    doc.text('Archives Of Us Coffee Inc', 200, tableTop + 15);
    doc.text(data.invoiceNumber, 350, tableTop + 15);

    doc.font('Helvetica-Bold').text('Tracking #', 50, tableTop + 40);
    doc.font('Helvetica-Bold').text('Due date', 350, tableTop + 40);
    doc.font('Helvetica').text(data.dueDate, 350, tableTop + 55);

    // Line items table
    doc.moveDown(4);
    const itemsTop = doc.y + 20;
    
    // Table header
    doc.fillColor('#f0f0f0').rect(50, itemsTop, 500, 20).fill();
    doc.fillColor('#000000').font('Helvetica-Bold').fontSize(10);
    doc.text('Description', 55, itemsTop + 5);
    doc.text('Qty', 280, itemsTop + 5);
    doc.text('Unit price', 350, itemsTop + 5);
    doc.text('Total price', 450, itemsTop + 5);

    // Table rows
    let rowY = itemsTop + 25;
    doc.font('Helvetica');
    
    for (const item of data.items) {
      doc.text(item.description, 55, rowY);
      doc.text(item.quantity.toString(), 280, rowY);
      doc.text(`$${item.unitPrice.toFixed(2)}`, 350, rowY);
      doc.text(`$${item.total.toFixed(2)}`, 450, rowY);
      rowY += 20;
    }

    // Bank info and totals
    doc.moveDown(4);
    const bankY = rowY + 30;
    
    doc.font('Helvetica-Bold').fontSize(9).text('Bank Information for Payment:', 50, bankY);
    doc.font('Helvetica').fontSize(9)
      .text('- Bank Name: CHASE BANK', 50, bankY + 15)
      .text('- Account Number: 2906513172', 50, bankY + 28)
      .text('- Routing Number: 322271627', 50, bankY + 41)
      .text('- Account Holder Name: ARCHIVES OF US COFFEE INC', 50, bankY + 54);

    // Totals on right side
    doc.font('Helvetica-Bold').fontSize(10);
    doc.text('Subtotal', 380, bankY);
    doc.text(`$${data.subtotal.toFixed(2)}`, 450, bankY);
    
    doc.text('Adjustments', 380, bankY + 20);
    
    doc.fontSize(12);
    doc.text(`$${data.total.toFixed(2)}`, 450, bankY + 45);

    doc.end();

    writeStream.on('finish', resolve);
    writeStream.on('error', reject);
  });
}

// ============ AI Processing ============

app.post('/api/process', async (req, res) => {
  try {
    // Always fetch fresh inventory from Google Sheets before processing
    await ensureFreshInventory();
    
    const { text, context, conversationState } = req.body;
    
    if (!text) {
      return res.status(400).json({ error: 'Text is required' });
    }
    
    const textLower = text.toLowerCase().trim();
    
    // Quick handlers for simple commands (no AI needed)
    
    // Handle simple inventory check commands
    if (textLower === 'inventory' || textLower === 'check inventory' || textLower === 'stock' || textLower === 'check stock') {
      return res.json({
        response: null,  // Frontend will handle display
        action: 'check_inventory',
        showFollowUp: true
      });
    }
    
    // Only handle simple yes/no/thanks without AI (for speed)
    if (textLower === 'yes' || textLower === 'yeah' || textLower === 'yep') {
      if (conversationState === 'waiting_for_new_customer_confirmation') {
        return res.json({
          response: "Great! Adding them now...",
          action: 'confirm_add_customer',
          showFollowUp: false
        });
      }
    }
    
    if (textLower === 'thanks' || textLower === 'thank you' || textLower === "that's all") {
      return res.json({
        response: "You're welcome!",
        action: 'completed',
        showFollowUp: true
      });
    }
    
    // Build comprehensive context for Gemini - let it handle everything else
    const customerDetails = Object.entries(customers).map(([name, data]) => 
      `${name} (${data.code}): ${data.email || 'no email'}`
    ).join('\n');
    
    const roastedSummary = roastedCoffeeInventory.length > 0 
      ? roastedCoffeeInventory.map(c => `- ${c.name}: ${c.weight} lb`).join('\n')
      : '- None in stock';
      
    const greenSummary = greenCoffeeInventory.length > 0
      ? greenCoffeeInventory.map(c => `- ${c.name}: ${c.weight} lb`).join('\n')
      : '- None in stock';
      
    const enRouteSummary = enRouteCoffeeInventory.length > 0
      ? enRouteCoffeeInventory.map(c => `- ${c.name}: ${c.weight} lb (tracking: ${c.trackingNumber || 'none'})`).join('\n')
      : '- Nothing en route';
    
    // Gemini-first approach - let it handle everything with full context
    let intentData = null;
    try {
      const intentPrompt = `You are Mise, an AI assistant for Archives of Us Coffee inventory and invoicing.

=== CURRENT DATA ===

ROASTED COFFEE INVENTORY:
${roastedSummary}

GREEN COFFEE INVENTORY (unroasted beans):
${greenSummary}

EN ROUTE (shipped, not yet delivered):
${enRouteSummary}

CUSTOMERS:
${customerDetails}

CONVERSATION STATE: ${conversationState || 'none'}

=== USER MESSAGE ===
"${text}"

=== YOUR TASK ===
Understand what the user wants and respond helpfully.

AVAILABLE ACTIONS:
- "check_inventory": User asking about stock levels, inventory, how much of something
- "create_invoice": User wants to invoice a customer (needs customer + quantity + product)
- "order_roast": User wants to place a roast order
- "view_en_route": User asking about shipments or tracking
- "decline": User canceling, saying never mind, changing topics, or saying no/cancel/nope
- "general": Conversation, questions, or unclear requests

GUIDELINES:
1. If user asks about inventory, provide the ACTUAL DATA from above
2. If user asks about "green coffee" or "unroasted", show GREEN COFFEE data
3. If unclear, ask a clarifying question - don't say you don't understand
4. If you ask a question, set needsFollowUp to true

Respond with JSON only:
{
  "intent": "<action>",
  "response": "<your helpful response with actual data>",
  "customer": "<customer name or null>",
  "items": [{"quantity": <number>, "product": "<product>"}],
  "isKnownCustomer": <true/false>,
  "needsFollowUp": <true if you asked a question>
}`;

      const intentText = await callGeminiWithRetry(intentPrompt, { temperature: 0.2, maxRetries: 2 });
      console.log('🤖 Gemini response:', intentText);
      
      const cleanJson = intentText.replace(/```json\n?|\n?```/g, '').trim();
      intentData = JSON.parse(cleanJson);
      
    } catch (error) {
      if (error.message === 'RATE_LIMITED') {
        console.log('⚠️ Rate limited');
        return res.json({
          response: "I'm a bit busy right now. Please try again in a moment.",
          action: 'rate_limited',
          showFollowUp: true
        });
      } else {
        console.error('Gemini error:', error);
        return res.json({
          response: "Sorry, I didn't get that. What can I help you with?",
          action: 'unclear',
          showFollowUp: false
        });
      }
    }
    
    if (!intentData) {
      return res.json({ 
        response: "Sorry, I didn't get that. What can I help you with?",
        action: 'unclear',
        showFollowUp: false
      });
    }
    
    // Use Gemini's response directly - it has all the data
    const intent = intentData.intent;
    const geminiResponse = intentData.response;
    const needsFollowUp = intentData.needsFollowUp;
    
    // Handle check_inventory - use Gemini's response
    if (intent === 'check_inventory') {
      return res.json({
        response: geminiResponse,
        action: 'check_inventory',
        showFollowUp: !needsFollowUp
      });
    }
    
    // Handle decline
    if (intent === 'decline') {
      return res.json({
        response: geminiResponse || "No problem!",
        action: 'declined',
        showFollowUp: true
      });
    }
    
    // Handle general questions
    if (intent === 'general') {
      return res.json({
        response: geminiResponse,
        action: 'general',
        showFollowUp: !needsFollowUp
      });
    }
    
    // Handle order_roast intent
    if (intent === 'order_roast') {
      return res.json({
        response: geminiResponse || "I can help you order roasts. Use the 'Order Roast' button to get started, or tell me which coffee you'd like to order.",
        action: 'order_roast',
        showFollowUp: !needsFollowUp
      });
    }
    
    // Handle view_en_route intent
    if (intent === 'view_en_route') {
      return res.json({
        response: geminiResponse || "Use the 'En Route' button to view your shipped orders and tracking information.",
        action: 'view_en_route',
        showFollowUp: !needsFollowUp
      });
    }
    
    // Handle confirm (when user types "yes" to add new customer)
    if (intent === 'confirm' && conversationState === 'waiting_for_new_customer_confirmation') {
      return res.json({
        response: "Great! Adding them now...",
        action: 'confirm_add_customer',
        showFollowUp: false
      });
    }
    
    // Handle different intents
    if (intentData.intent === 'create_invoice') {
      // Check if we have customer and at least one item
      const hasItems = intentData.items && intentData.items.length > 0;
      const hasLegacyFormat = intentData.quantity && intentData.product;
      
      if (intentData.customer && (hasItems || hasLegacyFormat)) {
        // Do our own customer matching (don't rely on Gemini's isKnownCustomer)
        const customerLower = intentData.customer.toLowerCase();
        const matchedKnownCustomer = getKnownCustomers().find(c => 
          c.toLowerCase() === customerLower ||
          customerLower.includes(c.toLowerCase()) ||
          c.toLowerCase().includes(customerLower)
        );
        
        const isActuallyKnown = !!matchedKnownCustomer;
        const customerToUse = matchedKnownCustomer || intentData.customer;
        
        console.log(`📋 Customer check: "${intentData.customer}" → matched: "${matchedKnownCustomer}", isKnown: ${isActuallyKnown}`);
        
        // Build items description for response
        let itemsDesc = '';
        if (hasItems) {
          itemsDesc = intentData.items.map(item => `${item.quantity} lbs ${item.product}`).join(', ');
        } else {
          itemsDesc = `${intentData.quantity} ${intentData.unit || 'lbs'} of ${intentData.product}`;
        }
        
        if (!isActuallyKnown) {
          // Unknown customer - ask to add
          return res.json({
            response: `I don't recognize "${intentData.customer}" as a current wholesale client. Would you like me to add them as a new customer?`,
            action: 'confirm_new_customer',
            pendingInvoice: {
              customer: intentData.customer,
              originalText: text
            },
            showFollowUp: false
          });
        } else {
          // Known customer - proceed with invoice, pass original text for accurate parsing
          return res.json({
            response: `Got it! Creating an invoice for ${customerToUse}: ${itemsDesc}. Processing now...`,
            action: 'create_invoice',
            invoiceDetails: text,  // Pass original text for multi-item parsing
            showFollowUp: false
          });
        }
      } else {
        // Missing info for invoice
        return res.json({
          response: geminiResponse || "I'd be happy to create an invoice! Could you provide the customer name, quantity, and product?",
          action: 'need_more_info',
          showFollowUp: false
        });
      }
    }
    
    // For other intents, return Gemini's response
    res.json({ 
      response: geminiResponse || "How can I help you today?",
      action: intent,
      data: intentData,
      showFollowUp: !needsFollowUp
    });
  } catch (error) {
    console.error('AI processing error:', error);
    res.status(500).json({ error: 'Failed to process', details: error.message });
  }
});

// ============ WebSocket for Transcription ============

wss.on('connection', async (clientWs) => {
  console.log('Client connected for transcription');
  
  let transcribeStream = null;
  let audioStream = null;
  let isTranscribing = false;
  
  clientWs.on('message', async (message) => {
    if (!Buffer.isBuffer(message)) return;

    if (!isTranscribing && !transcribeStream) {
      try {
        isTranscribing = true;
        audioStream = new PassThrough();
        audioStream.setMaxListeners(0);
        
        const sessionId = crypto.randomBytes(16).toString('hex');
        
        const audioBuffer = Buffer.isBuffer(message) ? message : Buffer.from(message);
        audioStream.write(audioBuffer);
        
        // Use standard transcription (not medical)
        const command = new StartStreamTranscriptionCommand({
          LanguageCode: 'en-US',
          MediaSampleRateHertz: 16000,
          MediaEncoding: 'pcm',
          AudioStream: (async function* () {
            for await (const chunk of audioStream) {
              yield { AudioEvent: { AudioChunk: chunk } };
            }
          })()
        });

        const response = await transcribeClient.send(command);
        transcribeStream = response.TranscriptResultStream;
        
        console.log('✅ Transcription session started:', sessionId);

        (async () => {
          try {
            for await (const event of transcribeStream) {
              if (event.TranscriptEvent) {
                const results = event.TranscriptEvent.Transcript.Results;
                
                for (const result of results) {
                  const transcript = result.Alternatives[0]?.Transcript;
                  
                  if (transcript && transcript.trim()) {
                    if (clientWs.readyState === WebSocket.OPEN) {
                      clientWs.send(JSON.stringify({
                        type: 'transcript',
                        text: transcript,
                        is_final: !result.IsPartial
                      }));
                    }
                  }
                }
              }
            }
          } catch (error) {
            if (error.name !== 'AbortError') {
              console.error('Transcription stream error:', error.message);
            }
          } finally {
            isTranscribing = false;
            transcribeStream = null;
          }
        })();
        
      } catch (error) {
        console.error('Error starting transcription:', error);
        isTranscribing = false;
        transcribeStream = null;
        
        if (clientWs.readyState === WebSocket.OPEN) {
          clientWs.send(JSON.stringify({
            type: 'error',
            message: 'Transcription error: ' + error.message
          }));
        }
      }
    } else if (audioStream && isTranscribing) {
      try {
        const audioBuffer = Buffer.isBuffer(message) ? message : Buffer.from(message);
        audioStream.write(audioBuffer);
      } catch (error) {
        console.error('Error sending audio:', error);
      }
    }
  });

  clientWs.on('close', () => {
    console.log('Client disconnected');
    isTranscribing = false;
    if (audioStream) {
      audioStream.end();
      audioStream = null;
    }
    transcribeStream = null;
  });

  clientWs.on('error', (error) => {
    console.error('WebSocket error:', error);
    isTranscribing = false;
    if (audioStream) {
      audioStream.end();
      audioStream = null;
    }
    transcribeStream = null;
  });
});

// ============ Coffee Inventory API Endpoints ============

// Get all inventory
app.get('/api/inventory', async (req, res) => {
  await ensureFreshInventory();
  res.json({
    green: greenCoffeeInventory,
    roasted: roastedCoffeeInventory,
    enRoute: enRouteCoffeeInventory
  });
});

// Get inventory summary formatted
app.get('/api/inventory/summary', async (req, res) => {
  await ensureFreshInventory();
  res.json({ summary: formatInventorySummary() });
});

// Get green coffee inventory
app.get('/api/inventory/green', async (req, res) => {
  await ensureFreshInventory();
  res.json(greenCoffeeInventory);
});

// Update green coffee inventory
app.post('/api/inventory/green/update', async (req, res) => {
  await ensureFreshInventory();
  const { id, weight, roastProfile, dropTemp } = req.body;
  const coffee = greenCoffeeInventory.find(c => c.id === id);
  if (!coffee) {
    return res.status(404).json({ error: 'Green coffee not found' });
  }
  // Prevent negative inventory
  if (weight !== undefined && weight < 0) {
    return res.json({ 
      success: false, 
      error: 'invalid_weight',
      message: `Cannot set negative inventory. Current ${coffee.name} weight is ${coffee.weight}lb. What would you like to do?`
    });
  }
  if (weight !== undefined) coffee.weight = weight;
  if (roastProfile !== undefined) coffee.roastProfile = roastProfile;
  if (dropTemp !== undefined) coffee.dropTemp = dropTemp;
  await syncInventoryToSheets();
  res.json({ success: true, coffee, message: `${coffee.name} updated to ${coffee.weight}lb. What else can I help you with?` });
});

// Add new green coffee
app.post('/api/inventory/green/add', async (req, res) => {
  await ensureFreshInventory();
  const { name, weight, roastProfile, dropTemp } = req.body;
  const id = name.toLowerCase().replace(/\s+/g, '-');
  if (greenCoffeeInventory.find(c => c.id === id)) {
    return res.status(400).json({ error: 'Coffee already exists' });
  }
  const newCoffee = { id, name, weight, roastProfile, dropTemp };
  greenCoffeeInventory.push(newCoffee);
  await syncInventoryToSheets();
  res.json({ success: true, coffee: newCoffee });
});

// Remove green coffee
app.post('/api/inventory/green/remove', async (req, res) => {
  await ensureFreshInventory();
  const { id } = req.body;
  const index = greenCoffeeInventory.findIndex(c => c.id === id);
  if (index === -1) {
    return res.status(404).json({ error: 'Green coffee not found' });
  }
  greenCoffeeInventory.splice(index, 1);
  await syncInventoryToSheets();
  res.json({ success: true });
});

// Get roasted coffee inventory
app.get('/api/inventory/roasted', async (req, res) => {
  await ensureFreshInventory();
  res.json(roastedCoffeeInventory);
});

// Update roasted coffee inventory
app.post('/api/inventory/roasted/update', async (req, res) => {
  await ensureFreshInventory();
  const { id, weight, type, recipe } = req.body;
  const coffee = roastedCoffeeInventory.find(c => c.id === id);
  if (!coffee) {
    return res.status(404).json({ error: 'Roasted coffee not found' });
  }
  // Prevent negative inventory
  if (weight !== undefined && weight < 0) {
    return res.json({ 
      success: false, 
      error: 'invalid_weight',
      message: `Cannot set negative inventory. Current ${coffee.name} weight is ${coffee.weight}lb. What would you like to do?`
    });
  }
  if (weight !== undefined) coffee.weight = weight;
  if (type !== undefined) coffee.type = type;
  if (recipe !== undefined) coffee.recipe = recipe;
  await syncInventoryToSheets();
  res.json({ success: true, coffee, message: `${coffee.name} updated to ${coffee.weight}lb. What else can I help you with?` });
});

// Add new roasted coffee
app.post('/api/inventory/roasted/add', async (req, res) => {
  await ensureFreshInventory();
  const { name, weight, type, recipe } = req.body;
  const id = name.toLowerCase().replace(/\s+/g, '-') + '-roasted';
  const newCoffee = { id, name, weight, type, recipe };
  roastedCoffeeInventory.push(newCoffee);
  await syncInventoryToSheets();
  res.json({ success: true, coffee: newCoffee });
});

// Remove roasted coffee
app.post('/api/inventory/roasted/remove', async (req, res) => {
  await ensureFreshInventory();
  const { id } = req.body;
  const index = roastedCoffeeInventory.findIndex(c => c.id === id);
  if (index === -1) {
    return res.status(404).json({ error: 'Roasted coffee not found' });
  }
  roastedCoffeeInventory.splice(index, 1);
  await syncInventoryToSheets();
  res.json({ success: true });
});

// Get en route inventory
app.get('/api/inventory/enroute', async (req, res) => {
  await ensureFreshInventory();
  res.json(enRouteCoffeeInventory);
});

// Add to en route inventory
app.post('/api/inventory/enroute/add', async (req, res) => {
  await ensureFreshInventory();
  const { name, weight, type, recipe, orderDate } = req.body;
  const id = `enroute-${Date.now()}`;
  
  // Format date as mm/dd/yy
  let formattedDate = orderDate;
  if (!formattedDate) {
    const now = new Date();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const year = String(now.getFullYear()).slice(-2);
    formattedDate = `${month}/${day}/${year}`;
  }
  
  const newItem = {
    id,
    name,
    weight,
    type,
    recipe,
    trackingNumber: '',
    dateOrdered: formattedDate,
    estimatedDelivery: ''
  };
  enRouteCoffeeInventory.push(newItem);
  await syncInventoryToSheets();
  res.json({ success: true, item: newItem });
});

// Update tracking number and fetch estimated delivery
app.post('/api/inventory/enroute/tracking', async (req, res) => {
  await ensureFreshInventory();
  const { id, trackingNumber, manualDeliveryDate } = req.body;
  const item = enRouteCoffeeInventory.find(c => c.id === id);
  if (!item) {
    return res.status(404).json({ error: 'En route item not found' });
  }
  item.trackingNumber = trackingNumber;
  
  // If user provided a manual delivery date, use it
  if (manualDeliveryDate) {
    item.estimatedDelivery = manualDeliveryDate;
  }
  
  const trackingUrl = `https://www.ups.com/track?tracknum=${trackingNumber}`;
  
  await syncInventoryToSheets();
  
  // Build response message
  let message = `Tracking saved: ${trackingNumber}`;
  if (item.estimatedDelivery) {
    message += `\n📦 Est. Delivery: ${item.estimatedDelivery}`;
  }
  message += `\n\nTrack on UPS: ${trackingUrl}`;
  
  res.json({ 
    success: true, 
    item,
    trackingUrl,
    message
  });
});

// Mark en route item as delivered (moves to roasted inventory)
app.post('/api/inventory/enroute/deliver', async (req, res) => {
  await ensureFreshInventory();
  const { id, confirmedBy } = req.body;
  const index = enRouteCoffeeInventory.findIndex(c => c.id === id);
  if (index === -1) {
    return res.status(404).json({ error: 'En route item not found' });
  }
  
  const item = enRouteCoffeeInventory[index];
  
  // Log the delivery confirmation
  console.log(`Delivery confirmed: ${item.name} (${item.weight}lb) by ${confirmedBy || 'unknown'} at ${new Date().toISOString()}`);
  
  // Check if this roasted coffee already exists
  const existingRoasted = roastedCoffeeInventory.find(c => c.name === item.name);
  if (existingRoasted) {
    // Add weight to existing
    existingRoasted.weight += item.weight;
  } else {
    // Create new roasted coffee entry
    const newRoasted = {
      id: item.name.toLowerCase().replace(/\s+/g, '-') + '-roasted',
      name: item.name,
      weight: item.weight,
      type: item.type,
      recipe: item.recipe
    };
    roastedCoffeeInventory.push(newRoasted);
  }
  
  // Remove from en route
  enRouteCoffeeInventory.splice(index, 1);
  
  await syncInventoryToSheets();
  res.json({ success: true, message: `${item.name} (${item.weight}lb) added to roasted inventory. What else can I help you with?` });
});

// ============ UPS Tracking API ============

// Format date as mm/dd/yy
function formatDateMMDDYY(date) {
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const year = String(date.getFullYear()).slice(-2);
  return `${month}/${day}/${year}`;
}

// Look up UPS tracking info
async function getUPSEstimatedDelivery(trackingNumber) {
  if (!trackingNumber) return null;
  
  // Validate tracking number format
  const isValidFormat = trackingNumber.startsWith('1Z') || 
                        trackingNumber.match(/^\d{18,22}$/) || // UPS Mail Innovations
                        trackingNumber.match(/^T\d+$/); // UPS Freight
  
  if (!isValidFormat) {
    return {
      estimatedDelivery: null,
      status: 'Invalid tracking format',
      validFormat: false,
      message: 'This doesn\'t look like a valid UPS tracking number. UPS tracking numbers typically start with "1Z".'
    };
  }
  
  const trackingUrl = `https://www.ups.com/track?tracknum=${trackingNumber}`;
  
  // Manual entry - AI cannot access UPS tracking data directly
  // User will enter delivery date after checking UPS website
  return {
    estimatedDelivery: null,
    status: 'Saved',
    validFormat: true,
    hasDeliveryDate: false,
    trackingUrl
  };
}

// Look up UPS tracking info
app.post('/api/ups/track', async (req, res) => {
  const { trackingNumber } = req.body;
  
  if (!trackingNumber) {
    return res.json({ error: 'No tracking number provided' });
  }
  
  const trackingInfo = await getUPSEstimatedDelivery(trackingNumber);
  
  res.json({
    trackingNumber,
    status: trackingInfo?.status || 'Check UPS.com for status',
    estimatedDelivery: trackingInfo?.estimatedDelivery || null,
    validFormat: trackingInfo?.validFormat,
    hasDeliveryDate: trackingInfo?.hasDeliveryDate || false,
    trackingUrl: `https://www.ups.com/track?tracknum=${trackingNumber}`
  });
});

// Tracking lookup endpoint for chat display
app.post('/api/tracking/lookup', async (req, res) => {
  const { trackingNumber } = req.body;
  
  if (!trackingNumber) {
    return res.json({ success: false, error: 'No tracking number provided' });
  }
  
  const trackingInfo = await getUPSEstimatedDelivery(trackingNumber);
  
  res.json({
    success: true,
    tracking: {
      trackingNumber,
      status: trackingInfo?.status || 'In Transit',
      estimatedDelivery: trackingInfo?.estimatedDelivery || null,
      hasDeliveryDate: trackingInfo?.hasDeliveryDate || false,
      lastUpdate: trackingInfo?.lastUpdate || null,
      trackingUrl: `https://www.ups.com/track?tracknum=${trackingNumber}`
    }
  });
});

// ============ Retail Sales Management API ============

// Helper to get week date range string (MM/DD-MM/DD format) for a given date
function getWeekRangeString(date) {
  const d = new Date(date);
  const day = d.getDay();
  const start = new Date(d);
  start.setDate(d.getDate() - day); // Sunday
  const end = new Date(start);
  end.setDate(start.getDate() + 6); // Saturday
  
  const formatDate = (dt) => {
    return `${(dt.getMonth() + 1).toString().padStart(2, '0')}/${dt.getDate().toString().padStart(2, '0')}`;
  };
  
  return `${formatDate(start)}-${formatDate(end)}`;
}

// Helper to parse week range string back to end date
function parseWeekRangeEndDate(weekStr) {
  // Format: "11/28-12/04" or "12/05-12/11"
  const parts = weekStr.split('-');
  if (parts.length !== 2) return null;
  
  const currentYear = new Date().getFullYear();
  const endParts = parts[1].split('/');
  
  if (endParts.length !== 2) return null;
  
  const endMonth = parseInt(endParts[0]) - 1;
  const endDay = parseInt(endParts[1]);
  
  // Determine year based on context
  const startParts = parts[0].split('/');
  const startMonth = parseInt(startParts[0]) - 1;
  
  let endYear = currentYear;
  // Handle year rollover (e.g., 12/28-01/03)
  if (endMonth < startMonth) {
    endYear = currentYear + 1;
  }
  
  return new Date(endYear, endMonth, endDay);
}

// Get retail data (products and weeks)
app.get('/api/retail/data', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    // Read the Retail Sales sheet - use valueRenderOption to get calculated values
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Retail Sales!A1:Z100',
      valueRenderOption: 'UNFORMATTED_VALUE' // Get actual numbers, not formatted strings
    });
    
    const rows = response.data.values || [];
    
    // Row 2 (index 1) has headers
    const headerRow = rows[1] || [];
    
    // Find column indices dynamically
    const products = [];
    let totalColIndex = -1;
    let productStartColIndex = -1;
    
    for (let i = 0; i < headerRow.length; i++) {
      const header = headerRow[i];
      if (header === 'Total Retail Sales') {
        totalColIndex = i;
        break;
      }
      // First non-empty, non-Date header is the start of products
      if (header && header !== 'Date' && header !== '') {
        if (productStartColIndex === -1) {
          productStartColIndex = i;
        }
        products.push({
          index: i,
          name: header,
          column: String.fromCharCode(65 + i)
        });
      }
    }
    
    // Get existing weeks (rows 3+, index 2+)
    const weeks = [];
    for (let i = 2; i < rows.length; i++) {
      const row = rows[i];
      if (!row) continue;
      
      // Find the date - it should be in column B (index 1), but check a few positions
      let dateStr = row[1]; // Column B
      if (!dateStr && row[0]) dateStr = row[0]; // Fallback to A if B is empty
      
      if (!dateStr) continue; // Skip rows without a date
      
      const weekData = {
        rowIndex: i + 1, // Excel row (1-indexed)
        dateRange: String(dateStr),
        sales: {}
      };
      
      // Get sales for each product
      let hasAnySales = false;
      products.forEach((product) => {
        const value = row[product.index];
        const numVal = (value !== undefined && value !== null && value !== '') ? parseFloat(value) : null;
        weekData.sales[product.name] = numVal;
        if (numVal !== null && !isNaN(numVal) && numVal !== 0) hasAnySales = true;
      });
      
      weekData.hasData = hasAnySales;
      
      // Get totals if available
      if (totalColIndex > -1) {
        const totalVal = row[totalColIndex];
        const feeVal = row[totalColIndex + 1];
        const netVal = row[totalColIndex + 2];
        
        weekData.totalSales = (totalVal !== undefined && totalVal !== null && totalVal !== '') ? parseFloat(totalVal) : 0;
        weekData.transactionFee = (feeVal !== undefined && feeVal !== null && feeVal !== '') ? parseFloat(feeVal) : 0;
        weekData.netPayout = (netVal !== undefined && netVal !== null && netVal !== '') ? parseFloat(netVal) : 0;
      }
      
      weeks.push(weekData);
    }
    
    // Calculate weeks that need to be added up to current date
    const today = new Date();
    const currentWeekRange = getWeekRangeString(today);
    
    // Find the last week end date in the sheet
    let lastWeekEndDate = null;
    if (weeks.length > 0) {
      const lastWeek = weeks[weeks.length - 1];
      lastWeekEndDate = parseWeekRangeEndDate(lastWeek.dateRange);
    }
    
    // Generate missing weeks
    const missingWeeks = [];
    if (lastWeekEndDate) {
      let nextWeekStart = new Date(lastWeekEndDate);
      nextWeekStart.setDate(nextWeekStart.getDate() + 1);
      
      // Add weeks until we reach the current week
      while (nextWeekStart <= today) {
        const weekRange = getWeekRangeString(nextWeekStart);
        // Don't add if it's already in the list
        if (!weeks.find(w => w.dateRange === weekRange) && !missingWeeks.includes(weekRange)) {
          missingWeeks.push(weekRange);
        }
        nextWeekStart.setDate(nextWeekStart.getDate() + 7);
      }
    }
    
    // Find weeks without sales data (incomplete)
    const incompleteWeeks = weeks.filter(w => !w.hasData);
    
    res.json({
      products,
      weeks,
      missingWeeks,
      incompleteWeeks,
      currentWeek: currentWeekRange,
      totalColIndex,
      productStartColIndex,
      lastDataRow: weeks.length > 0 ? weeks[weeks.length - 1].rowIndex : 2
    });
    
  } catch (error) {
    console.error('Retail data fetch error:', error);
    res.status(500).json({ error: 'Failed to fetch retail data: ' + error.message });
  }
});

// Add missing weeks to the sheet
app.post('/api/retail/add-weeks', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  const { weeks } = req.body; // Array of week range strings to add
  
  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    // Get current sheet structure
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Retail Sales!A1:Z100'
    });
    
    const rows = response.data.values || [];
    const headerRow = rows[1] || [];
    
    // Find column indices dynamically
    let totalColIndex = -1;
    let productStartColIndex = -1;
    let productEndColIndex = -1;
    
    for (let i = 0; i < headerRow.length; i++) {
      const header = headerRow[i];
      if (header === 'Total Retail Sales') {
        totalColIndex = i;
        productEndColIndex = i - 1;
        break;
      }
      // First non-empty, non-Date header is the start of products
      if (header && header !== 'Date' && header !== '' && productStartColIndex === -1) {
        productStartColIndex = i;
      }
    }
    
    if (totalColIndex === -1 || productStartColIndex === -1) {
      return res.status(400).json({ error: 'Could not find sheet structure' });
    }
    
    // Find last data row
    let lastDataRow = 2;
    for (let i = 2; i < rows.length; i++) {
      if (rows[i] && rows[i][1]) {
        lastDataRow = i + 1;
      }
    }
    
    // Build rows to append - just date and empty cells, no formulas yet
    // Formulas will be added when user enters sales data
    const newRows = weeks.map((weekRange, idx) => {
      const row = [];
      
      // Fill columns up to and including net payout column
      for (let i = 0; i <= totalColIndex + 2; i++) {
        if (i === 1) {
          // Column B = date
          row.push(weekRange);
        } else {
          // Empty cell - no formulas for empty weeks
          row.push('');
        }
      }
      
      return row;
    });
    
    // Append the new rows
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: `Retail Sales!A${lastDataRow + 1}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: newRows
      }
    });
    
    res.json({ 
      success: true, 
      message: `Added ${weeks.length} week(s) to Retail Sales sheet`,
      weeksAdded: weeks
    });
    
  } catch (error) {
    console.error('Add weeks error:', error);
    res.status(500).json({ error: 'Failed to add weeks: ' + error.message });
  }
});

// Update sales for a specific week
app.post('/api/retail/sales', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  const { rowIndex, sales } = req.body; // rowIndex is 1-indexed, sales is {productName: value}
  
  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    // Get current header row to find column mappings
    const headerResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Retail Sales!A2:Z2'
    });
    
    const headerRow = headerResponse.data.values?.[0] || [];
    
    // Find Total Retail Sales column and product range
    let totalColIndex = -1;
    let productStartColIndex = -1;
    let productEndColIndex = -1;
    
    for (let i = 0; i < headerRow.length; i++) {
      const header = headerRow[i];
      if (header === 'Total Retail Sales') {
        totalColIndex = i;
        productEndColIndex = i - 1;
        break;
      }
      if (header && header !== 'Date' && header !== '' && productStartColIndex === -1) {
        productStartColIndex = i;
      }
    }
    
    // Build updates for sales values
    const updates = [];
    let hasValidSalesData = false;
    
    for (const [productName, value] of Object.entries(sales)) {
      const colIndex = headerRow.findIndex(h => h === productName);
      if (colIndex > -1) {
        const col = String.fromCharCode(65 + colIndex);
        const numValue = value !== null && value !== '' ? parseFloat(value) : '';
        updates.push({
          range: `Retail Sales!${col}${rowIndex}`,
          values: [[numValue]]
        });
        // Check if any valid sales data was entered
        if (numValue !== '' && !isNaN(numValue) && numValue > 0) {
          hasValidSalesData = true;
        }
      }
    }
    
    // Only add formulas for Total, Fee, and Net Payout if user entered sales data
    if (hasValidSalesData && totalColIndex > -1 && productStartColIndex > -1) {
      const startCol = String.fromCharCode(65 + productStartColIndex);
      const endCol = String.fromCharCode(65 + productEndColIndex);
      const totalCol = String.fromCharCode(65 + totalColIndex);
      const feeCol = String.fromCharCode(65 + totalColIndex + 1);
      const netCol = String.fromCharCode(65 + totalColIndex + 2);
      
      // Add formula updates
      updates.push({
        range: `Retail Sales!${totalCol}${rowIndex}`,
        values: [[`=SUM(${startCol}${rowIndex}:${endCol}${rowIndex})`]]
      });
      updates.push({
        range: `Retail Sales!${feeCol}${rowIndex}`,
        values: [[`=${totalCol}${rowIndex}*0.03`]]
      });
      updates.push({
        range: `Retail Sales!${netCol}${rowIndex}`,
        values: [[`=${totalCol}${rowIndex}-${feeCol}${rowIndex}`]]
      });
    }
    
    if (updates.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data: updates
        }
      });
      
      // Apply currency formatting to money columns (products through net payout)
      if (hasValidSalesData && totalColIndex > -1 && productStartColIndex > -1) {
        // Get sheet ID for Retail Sales
        const spreadsheet = await sheets.spreadsheets.get({
          spreadsheetId: SPREADSHEET_ID
        });
        const retailSheet = spreadsheet.data.sheets.find(s => s.properties.title === 'Retail Sales');
        
        if (retailSheet) {
          const sheetId = retailSheet.properties.sheetId;
          const netColIndex = totalColIndex + 2;
          
          // Apply currency format to the row (product columns through net payout)
          await sheets.spreadsheets.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            requestBody: {
              requests: [{
                repeatCell: {
                  range: {
                    sheetId: sheetId,
                    startRowIndex: rowIndex - 1, // 0-indexed
                    endRowIndex: rowIndex,
                    startColumnIndex: productStartColIndex,
                    endColumnIndex: netColIndex + 1
                  },
                  cell: {
                    userEnteredFormat: {
                      numberFormat: {
                        type: 'CURRENCY',
                        pattern: '"$"#,##0.00'
                      }
                    }
                  },
                  fields: 'userEnteredFormat.numberFormat'
                }
              }]
            }
          });
        }
      }
    }
    
    res.json({ 
      success: true, 
      message: 'Sales data updated',
      updatedCells: updates.length
    });
    
  } catch (error) {
    console.error('Update sales error:', error);
    res.status(500).json({ error: 'Failed to update sales: ' + error.message });
  }
});

// Add a new retail product
app.post('/api/retail/products/add', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  const { productName } = req.body;
  
  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    // Get current structure
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Retail Sales!A1:Z100'
    });
    
    const rows = response.data.values || [];
    const headerRow = rows[1] || [];
    
    // Find Total Retail Sales column
    let totalColIndex = -1;
    for (let i = 0; i < headerRow.length; i++) {
      if (headerRow[i] === 'Total Retail Sales') {
        totalColIndex = i;
        break;
      }
    }
    
    if (totalColIndex === -1) {
      return res.status(400).json({ error: 'Could not find Total Retail Sales column' });
    }
    
    // Get spreadsheet to find sheet ID
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId: SPREADSHEET_ID
    });
    
    const retailSheet = spreadsheet.data.sheets.find(s => s.properties.title === 'Retail Sales');
    if (!retailSheet) {
      return res.status(400).json({ error: 'Retail Sales sheet not found' });
    }
    
    const sheetId = retailSheet.properties.sheetId;
    
    // Insert column at totalColIndex (before Total Retail Sales)
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{
          insertDimension: {
            range: {
              sheetId: sheetId,
              dimension: 'COLUMNS',
              startIndex: totalColIndex,
              endIndex: totalColIndex + 1
            },
            inheritFromBefore: true
          }
        }]
      }
    });
    
    // Update the new column header
    const newCol = String.fromCharCode(65 + totalColIndex);
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `Retail Sales!${newCol}2`,
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: [[productName]]
      }
    });
    
    // Update Total formulas to include new column
    const newTotalCol = String.fromCharCode(65 + totalColIndex + 1);
    const startCol = 'C';
    const endCol = newCol;
    
    const formulaUpdates = [];
    for (let i = 3; i <= rows.length + 5; i++) { // Add some buffer
      if (rows[i - 1] && rows[i - 1][1]) { // Has date
        formulaUpdates.push({
          range: `Retail Sales!${newTotalCol}${i}`,
          values: [[`=SUM(${startCol}${i}:${endCol}${i})`]]
        });
      }
    }
    
    if (formulaUpdates.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data: formulaUpdates
        }
      });
    }
    
    res.json({ 
      success: true, 
      message: `Added product "${productName}"`,
      column: newCol
    });
    
  } catch (error) {
    console.error('Add product error:', error);
    res.status(500).json({ error: 'Failed to add product: ' + error.message });
  }
});

// Remove a retail product
app.post('/api/retail/products/remove', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  const { productName } = req.body;
  
  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    // Get current header
    const headerResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Retail Sales!A2:Z2'
    });
    
    const headerRow = headerResponse.data.values?.[0] || [];
    
    // Find the column
    const colIndex = headerRow.findIndex(h => h === productName);
    if (colIndex === -1) {
      return res.status(400).json({ error: `Product "${productName}" not found` });
    }
    
    // Get sheet ID
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId: SPREADSHEET_ID
    });
    
    const retailSheet = spreadsheet.data.sheets.find(s => s.properties.title === 'Retail Sales');
    if (!retailSheet) {
      return res.status(400).json({ error: 'Retail Sales sheet not found' });
    }
    
    const sheetId = retailSheet.properties.sheetId;
    
    // Delete the column
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{
          deleteDimension: {
            range: {
              sheetId: sheetId,
              dimension: 'COLUMNS',
              startIndex: colIndex,
              endIndex: colIndex + 1
            }
          }
        }]
      }
    });
    
    // Update Total formulas after deletion
    const dataResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Retail Sales!A1:Z100'
    });
    
    const rows = dataResponse.data.values || [];
    const newHeaderRow = rows[1] || [];
    
    // Find new total column index
    let newTotalColIndex = -1;
    for (let i = 0; i < newHeaderRow.length; i++) {
      if (newHeaderRow[i] === 'Total Retail Sales') {
        newTotalColIndex = i;
        break;
      }
    }
    
    if (newTotalColIndex > -1) {
      const totalCol = String.fromCharCode(65 + newTotalColIndex);
      const startCol = 'C';
      const endCol = String.fromCharCode(65 + newTotalColIndex - 1);
      
      const formulaUpdates = [];
      for (let i = 3; i <= rows.length; i++) {
        if (rows[i - 1] && rows[i - 1][1]) {
          formulaUpdates.push({
            range: `Retail Sales!${totalCol}${i}`,
            values: [[`=SUM(${startCol}${i}:${endCol}${i})`]]
          });
        }
      }
      
      if (formulaUpdates.length > 0) {
        await sheets.spreadsheets.values.batchUpdate({
          spreadsheetId: SPREADSHEET_ID,
          requestBody: {
            valueInputOption: 'USER_ENTERED',
            data: formulaUpdates
          }
        });
      }
    }
    
    res.json({ 
      success: true, 
      message: `Removed product "${productName}"`
    });
    
  } catch (error) {
    console.error('Remove product error:', error);
    res.status(500).json({ error: 'Failed to remove product: ' + error.message });
  }
});

// Rename a retail product
app.post('/api/retail/products/rename', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  const { oldName, newName } = req.body;
  
  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    // Get header row
    const headerResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Retail Sales!A2:Z2'
    });
    
    const headerRow = headerResponse.data.values?.[0] || [];
    
    // Find column
    const colIndex = headerRow.findIndex(h => h === oldName);
    if (colIndex === -1) {
      return res.status(400).json({ error: `Product "${oldName}" not found` });
    }
    
    const col = String.fromCharCode(65 + colIndex);
    
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `Retail Sales!${col}2`,
      valueInputOption: 'USER_ENTERED',
      requestBody: {
        values: [[newName]]
      }
    });
    
    res.json({ 
      success: true, 
      message: `Renamed "${oldName}" to "${newName}"`
    });
    
  } catch (error) {
    console.error('Rename product error:', error);
    res.status(500).json({ error: 'Failed to rename product: ' + error.message });
  }
});

// Format all sheets with currency formatting
app.post('/api/sheets/format-currency', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  try {
    await formatAllSheetsCurrency();
    res.json({ success: true, message: 'All sheets formatted with currency' });
  } catch (error) {
    console.error('Format currency error:', error);
    res.status(500).json({ error: 'Failed to format sheets: ' + error.message });
  }
});

// Fix/repair formulas for all retail rows
app.post('/api/retail/fix-formulas', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected' });
  }
  
  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    
    // Get current sheet structure
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Retail Sales!A1:Z100'
    });
    
    const rows = response.data.values || [];
    const headerRow = rows[1] || [];
    
    // Find column indices
    let totalColIndex = -1;
    let productStartColIndex = -1;
    let productEndColIndex = -1;
    
    for (let i = 0; i < headerRow.length; i++) {
      const header = headerRow[i];
      if (header === 'Total Retail Sales') {
        totalColIndex = i;
        productEndColIndex = i - 1;
        break;
      }
      if (header && header !== 'Date' && header !== '' && productStartColIndex === -1) {
        productStartColIndex = i;
      }
    }
    
    if (totalColIndex === -1 || productStartColIndex === -1) {
      return res.status(400).json({ error: 'Could not find sheet structure' });
    }
    
    const startCol = String.fromCharCode(65 + productStartColIndex);
    const endCol = String.fromCharCode(65 + productEndColIndex);
    const totalCol = String.fromCharCode(65 + totalColIndex);
    const feeCol = String.fromCharCode(65 + totalColIndex + 1);
    const netCol = String.fromCharCode(65 + totalColIndex + 2);
    
    // Build formula updates for all data rows
    const updates = [];
    let rowsFixed = 0;
    
    for (let i = 2; i < rows.length; i++) {
      const row = rows[i];
      if (!row) continue;
      
      // Check if row has a date (meaning it's a data row)
      const hasDate = row[1] || row[0];
      if (!hasDate) continue;
      
      const rowNum = i + 1; // 1-indexed
      
      updates.push({
        range: `Retail Sales!${totalCol}${rowNum}`,
        values: [[`=SUM(${startCol}${rowNum}:${endCol}${rowNum})`]]
      });
      updates.push({
        range: `Retail Sales!${feeCol}${rowNum}`,
        values: [[`=${totalCol}${rowNum}*0.03`]]
      });
      updates.push({
        range: `Retail Sales!${netCol}${rowNum}`,
        values: [[`=${totalCol}${rowNum}-${feeCol}${rowNum}`]]
      });
      
      rowsFixed++;
    }
    
    if (updates.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: {
          valueInputOption: 'USER_ENTERED',
          data: updates
        }
      });
    }
    
    res.json({ 
      success: true, 
      message: `Fixed formulas for ${rowsFixed} row(s)`,
      rowsFixed
    });
    
  } catch (error) {
    console.error('Fix formulas error:', error);
    res.status(500).json({ error: 'Failed to fix formulas: ' + error.message });
  }
});

// ============ Conversational Chat API ============

// Generate conversational response using ChatGPT (with sheet context)
app.post('/api/chat/respond', async (req, res) => {
  const { context, completedAction, inventory } = req.body;
  
  const systemPrompt = `You are Mise, a helpful assistant for Archives of Us Coffee inventory management. 
You have a warm, professional tone - friendly but efficient. Keep responses concise (1-2 sentences max).
Don't use emojis. Be conversational but professional.`;

  const userMessage = `${inventory ? `Current inventory summary:
- Green Coffee: ${inventory.green?.map(c => c.name).join(', ') || 'None'}
- Roasted Coffee: ${inventory.roasted?.map(c => c.name).join(', ') || 'None'}
- En Route: ${inventory.enRoute?.length || 0} items` : ''}

${context ? `Context: ${context}` : ''}
${completedAction ? `Just completed: ${completedAction}` : ''}

Generate a brief, natural follow-up message. If a task was just completed, acknowledge it and ask what else you can help with.`;

  try {
    const response = await callChatGPT(systemPrompt, userMessage, { temperature: 0.7 });
    res.json({ message: response });
  } catch (error) {
    console.error('Chat respond error:', error);
    // Fallback to Gemini if ChatGPT fails
    try {
      const fallbackResponse = await callGeminiWithRetry(systemPrompt + '\n\n' + userMessage);
      res.json({ message: fallbackResponse.trim() });
    } catch (e) {
      res.json({ message: 'What else can I help you with?' });
    }
  }
});

// Process general chat input using ChatGPT (with sheet visibility)
app.post('/api/chat/process', async (req, res) => {
  // Always fetch fresh inventory from Google Sheets before processing
  await ensureFreshInventory();
  
  const { userInput, currentState } = req.body;
  
  // Build sheet context so ChatGPT can see current inventory
  const sheetContext = await buildSheetContextForChatGPT();
  const contextStr = formatSheetContextForPrompt(sheetContext);
  
  const systemPrompt = `You are Mise, parsing user input for Archives of Us Coffee inventory management.
You have full visibility into the current inventory and can validate orders against actual stock.

Available actions:
- "inventory" or "check inventory" → Show current inventory
- "order roast" or mentions roasted coffee names → Start roast order
- "invoice" or "bill" → Generate invoice
- "en route" or "shipped" or "tracking" → View en route coffee
- "manage" or "edit inventory" → Open inventory manager

Current state: ${currentState || 'idle'}

${contextStr}

Determine what the user wants. Respond with JSON only:
{
  "action": "inventory|roast_order|invoice|en_route|manage|chat|unclear",
  "parameters": {},
  "chatResponse": "brief response if action is 'chat' or 'unclear'"
}`;

  try {
    const response = await callChatGPT(systemPrompt, userInput, { jsonMode: true });
    const parsed = JSON.parse(response);
    res.json(parsed);
  } catch (error) {
    console.error('Chat process error:', error);
    // Fallback to Gemini if ChatGPT fails
    try {
      const roastedCoffeeNames = roastedCoffeeInventory.map(c => c.name);
      const greenCoffeeNames = greenCoffeeInventory.map(c => c.name);
      const fallbackPrompt = `You are Mise, parsing user input for Archives of Us Coffee inventory management.

Available actions:
- "inventory" or "check inventory" → Show current inventory
- "order roast" or mentions roasted coffee names → Start roast order
- "invoice" or "bill" → Generate invoice
- "en route" or "shipped" or "tracking" → View en route coffee
- "manage" or "edit inventory" → Open inventory manager

Roasted coffees: ${JSON.stringify(roastedCoffeeNames)}
Green coffees: ${JSON.stringify(greenCoffeeNames)}

User said: "${userInput}"
Current state: ${currentState || 'idle'}

Determine what the user wants. Respond with JSON:
{
  "action": "inventory|roast_order|invoice|en_route|manage|chat|unclear",
  "parameters": {},
  "chatResponse": "brief response if action is 'chat' or 'unclear'"
}`;
      const fallbackResponse = await callGeminiWithRetry(fallbackPrompt);
      const jsonMatch = fallbackResponse.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        res.json(JSON.parse(jsonMatch[0]));
      } else {
        res.json({ action: 'chat', chatResponse: "Sorry, I didn't get that. What can I help you with?" });
      }
    } catch (e) {
      res.json({ action: 'chat', chatResponse: "I'm having trouble understanding. Could you try rephrasing?" });
    }
  }
});

// ============ Roast Order API Endpoints ============

// Parse roast order request using ChatGPT (with Gemini fallback)
app.post('/api/roast-order/parse', async (req, res) => {
  // Always fetch fresh inventory from Google Sheets before processing
  await ensureFreshInventory();
  
  const { userInput, previousQuestion, previousSuggestion } = req.body;
  
  const roastedCoffeeNames = roastedCoffeeInventory.map(c => c.name);
  const roastedCoffeeTypes = roastedCoffeeInventory.map(c => ({ name: c.name, type: c.type }));
  
  let contextInfo = '';
  if (previousQuestion && previousSuggestion) {
    contextInfo = `
PREVIOUS CONTEXT:
- Mise asked: "${previousQuestion}"
- Suggested coffee: "${previousSuggestion}"
- User now responded: "${userInput}"

If the user says "yes", "yeah", "correct", "that's right", "yep", etc., they are confirming the suggested coffee "${previousSuggestion}".
`;
  }
  
  const systemPrompt = `You are parsing a coffee roast order request for Archives of Us Coffee.

Available roasted coffees: ${JSON.stringify(roastedCoffeeNames)}
${contextInfo}

CRITICAL NICKNAME RECOGNITION - MATCH AUTOMATICALLY WITHOUT ASKING:
- "Blend", "Archives", "archive", "house blend" → Archives Blend
- "Ethiopia", "Ethiopi", "ethiopian", "Gera" → Ethiopia Gera (this is the ONLY Ethiopia in roasted coffee)
- "Decaf", "decaffeinated" → Colombia Decaf
- "Colombia", "Colombian", "Excelso" → Colombia Excelso

IMPORTANT RULES:
1. ALWAYS match nicknames automatically - do NOT ask for clarification if the nickname clearly maps to ONE coffee
2. "Archives and Ethiopia" means TWO coffees: Archives Blend AND Ethiopia Gera
3. "Archives and Ethiopi" (with typo) still means Archives Blend AND Ethiopia Gera
4. Parse ALL coffees mentioned in the request, not just one
5. If user confirms "yes" to a previous suggestion, return that coffee

Respond with JSON only:
{
  "understood": true/false,
  "needsClarification": true/false,
  "clarificationQuestion": "only ask if GENUINELY ambiguous",
  "suggestedCoffee": "only if asking for clarification",
  "coffees": [
    {"name": "Archives Blend", "matched": true},
    {"name": "Ethiopia Gera", "matched": true}
  ]
}

EXAMPLES:
- "Archives and Ethiopia" → coffees: [{name:"Archives Blend",matched:true}, {name:"Ethiopia Gera",matched:true}]
- "blend and ethiopi" → coffees: [{name:"Archives Blend",matched:true}, {name:"Ethiopia Gera",matched:true}]
- "just ethiopia" → coffees: [{name:"Ethiopia Gera",matched:true}]
- "archives" → coffees: [{name:"Archives Blend",matched:true}]`;

  try {
    // Try ChatGPT first
    const response = await callChatGPT(systemPrompt, userInput, { jsonMode: true });
    const parsed = JSON.parse(response);
    res.json(parsed);
  } catch (error) {
    console.error('ChatGPT parse error, falling back to Gemini:', error.message);
    // Fallback to Gemini
    try {
      const geminiPrompt = systemPrompt + `\n\nUser request: "${userInput}"`;
      const response = await callGeminiWithRetry(geminiPrompt);
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        const parsed = JSON.parse(jsonMatch[0]);
        res.json(parsed);
      } else {
        res.json({ understood: false, needsClarification: true, clarificationQuestion: "Which coffees would you like? For example: 'Archives Blend and Ethiopia Gera'" });
      }
    } catch (geminiError) {
      console.error('Gemini fallback also failed:', geminiError);
      res.json({ understood: false, needsClarification: true, clarificationQuestion: "Which coffees would you like? For example: 'Archives Blend and Ethiopia Gera'" });
    }
  }
});

// Modify roast order - parse user's modification request and recalculate
app.post('/api/roast-order/modify', async (req, res) => {
  // Fetch fresh inventory from Sheets
  await ensureFreshInventory();
  
  const { userRequest, currentOrder } = req.body;
  
  const roastedCoffeeNames = roastedCoffeeInventory.map(c => c.name);
  const roastedCoffeeTypes = roastedCoffeeInventory.map(c => ({ name: c.name, type: c.type }));
  
  const prompt = `You are Mise, an intelligent assistant for Archives of Us Coffee roast orders.

AVAILABLE ROASTED COFFEES: ${JSON.stringify(roastedCoffeeNames)}

CURRENT ORDER: ${JSON.stringify(currentOrder.map(o => ({ name: o.name, weight: o.weight })))}

USER REQUEST: "${userRequest}"

NICKNAME RECOGNITION - MATCH AUTOMATICALLY:
- "Blend", "Archives", "archive", "house" → Archives Blend
- "Ethiopia", "Ethiopi", "ethiopian", "Gera" → Ethiopia Gera
- "Colombia", "Colombian", "Excelso" → Colombia Excelso
- "Decaf" → Colombia Decaf

YOUR TASK:
1. Parse the user's request to understand what they want
2. If they say "also [coffee]" or "add [coffee]", ADD that coffee to the existing order
3. Extract coffee names and weights (amounts in pounds/lb)
4. Return the COMPLETE order (existing items + new items)

PARSING EXAMPLES:
- "also Archives" with current order Ethiopia 55lb → ADD Archives Blend, ask for weight
- "155lb" as response to "how many pounds of Archives" → Archives Blend: 155lb (KEEP existing Ethiopia 55lb)
- "80lb Archives Blend and 60lb Ethiopia" → Archives Blend: 80lb, Ethiopia Gera: 60lb
- "change archives to 100" → Keep other items, change Archives Blend to 100lb

IMPORTANT: 
- When user adds a coffee, KEEP the existing items and ADD the new one
- Return ALL items in the order, not just the modified one
- If asking for weight, set needsClarification: true

Respond with JSON only:
{
  "success": true/false,
  "items": [
    { "name": "exact coffee name", "weight": number_in_pounds }
  ],
  "needsClarification": true/false,
  "clarificationQuestion": "How many pounds of [coffee]?",
  "addingCoffee": "coffee name being added if asking for weight"
}`;

  try {
    const response = await callGeminiWithRetry(prompt);
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    
    if (!jsonMatch) {
      return res.json({ 
        success: false, 
        needsClarification: true,
        message: "I want to help you modify the order. Could you specify the amounts? For example: '80lb Archives Blend and 60lb Ethiopia Gera'"
      });
    }
    
    const parsed = JSON.parse(jsonMatch[0]);
    
    // If Gemini needs clarification (e.g., asking for weight), return that question
    if (parsed.needsClarification && parsed.clarificationQuestion) {
      return res.json({ 
        success: false, 
        needsClarification: true,
        message: parsed.clarificationQuestion,
        addingCoffee: parsed.addingCoffee
      });
    }
    
    if (!parsed.success || !parsed.items || parsed.items.length === 0) {
      return res.json({ 
        success: false, 
        needsClarification: true,
        message: parsed.clarificationQuestion || "How much of each coffee would you like? For example: '80lb Archives Blend and 60lb Ethiopia Gera'"
      });
    }
    
    // Helper to calculate batches
    // For defaults: use full 65lb batches to maximize batch weights
    // For user adjustments: distribute weight evenly across batches (25-65lb range)
    const calcBatches = (totalWeight, forceMax = false) => {
      const batches = Math.ceil(totalWeight / 65);
      if (forceMax) {
        // For default orders, use max 65lb batches
        return { batches, batchWeight: 65 };
      }
      // For user-specified weights, distribute evenly
      if (totalWeight <= 65) {
        return { batches: 1, batchWeight: Math.round(totalWeight) };
      }
      const batchWeight = Math.round(totalWeight / batches);
      return { batches, batchWeight };
    };
    
    // Build order items with proper recipe data
    const orderItems = [];
    let summaryHtml = '<strong>Order Summary:</strong><br><br>';
    
    for (const item of parsed.items) {
      const roastedCoffee = roastedCoffeeInventory.find(c => 
        c.name.toLowerCase() === item.name.toLowerCase() ||
        c.name.toLowerCase().includes(item.name.toLowerCase()) ||
        item.name.toLowerCase().includes(c.name.toLowerCase())
      );
      
      if (!roastedCoffee) continue;
      
      // Get recipe from inventory or use fallback
      let recipe = roastedCoffee.recipe;
      const nameLower = roastedCoffee.name.toLowerCase();
      
      // Fallback recipes if not in Sheet
      if (!recipe) {
        if (nameLower.includes('archives') || nameLower.includes('blend')) {
          recipe = [
            { greenCoffeeId: 'brazil-mogiano', name: 'Brazil Mogiano', percentage: 66.6667 },
            { greenCoffeeId: 'ethiopia-yirgacheffe', name: 'Ethiopia Yirgacheffe', percentage: 33.3333 }
          ];
        } else if (nameLower.includes('ethiopia') && nameLower.includes('gera')) {
          // Ethiopia Gera: 50/50 split between two lots
          recipe = [
            { greenCoffeeId: 'ethiopia-gera-58484', name: 'Ethiopia Gera 58484', percentage: 50 },
            { greenCoffeeId: 'ethiopia-gera-58479', name: 'Ethiopia Gera 58479', percentage: 50 }
          ];
        } else if (nameLower.includes('colombia') && !nameLower.includes('decaf')) {
          recipe = [{ greenCoffeeId: 'colombia-antioquia', name: 'Colombia Antioquia', percentage: 100 }];
        }
      }
      
      // Determine type
      let type = roastedCoffee.type;
      if (!type) {
        if (nameLower.includes('decaf')) type = 'Private Label';
        else if (recipe && recipe.length > 1) type = 'Blend';
        else type = 'Single Origin';
      }
      
      const orderItem = {
        name: roastedCoffee.name,
        type: type,
        recipe: recipe,
        weight: item.weight
      };
      orderItems.push(orderItem);
      
      // Generate summary HTML using FULL coffee names
      if (roastedCoffee.name === 'Archives Blend' && recipe) {
        // Archives Blend: Show Brazil batches blended with Yirgacheffe batches
        const totalGreenWeight = Math.round(item.weight / 0.85);
        const brazilComp = recipe.find(r => r.name.includes('Brazil'));
        const yirgComp = recipe.find(r => r.name.includes('Yirgacheffe'));
        
        const brazilGreen = greenCoffeeInventory.find(g => g.id === 'brazil-mogiano');
        const yirgGreen = greenCoffeeInventory.find(g => g.id === 'ethiopia-yirgacheffe');
        
        const brazilWeightNeeded = Math.round(totalGreenWeight * (brazilComp?.percentage || 66.67) / 100);
        const yirgWeightNeeded = Math.round(totalGreenWeight * (yirgComp?.percentage || 33.33) / 100);
        
        const { batches: brazilBatches, batchWeight: brazilBatchWeight } = calcBatches(brazilWeightNeeded);
        const { batches: yirgBatches, batchWeight: yirgBatchWeight } = calcBatches(yirgWeightNeeded);
        
        // Use user's entered weight, not recalculated
        summaryHtml += `<strong>Archives Blend</strong> (~${Math.round(item.weight)}lb roasted):<br>`;
        summaryHtml += `<div style="margin-left: 8px;">`;
        summaryHtml += `- ${brazilBatches} batch${brazilBatches > 1 ? 'es' : ''} of Brazil Mogiano (${brazilBatchWeight}lb - profile ${brazilGreen?.roastProfile || '199503'} - drop temp ${brazilGreen?.dropTemp || 419})<br>`;
        summaryHtml += `<span style="margin-left: 4px;">blended with</span><br>`;
        summaryHtml += `- ${yirgBatches} batch${yirgBatches > 1 ? 'es' : ''} of Ethiopia Yirgacheffe (${yirgBatchWeight}lb - profile ${yirgGreen?.roastProfile || '141402'} - drop temp ${yirgGreen?.dropTemp || 415})`;
        summaryHtml += `</div><br><br>`;
        
      } else if (roastedCoffee.name === 'Ethiopia Gera') {
        // Ethiopia Gera: Special 50/50 split between two lots per batch
        // Default batch is 65lb, split into 33lb + 33lb (round UP both - exception to max rule)
        const totalGreenWeight = Math.round(item.weight / 0.85);
        const { batches, batchWeight } = calcBatches(totalGreenWeight);
        const halfBatchWeight = Math.ceil(batchWeight / 2); // Round UP to 33lb each
        
        // Find either lot to get roast profile (they should be the same)
        const geraGreen = greenCoffeeInventory.find(g => g.id === 'ethiopia-gera-58484' || g.id === 'ethiopia-gera-58479' || g.id === 'ethiopia-gera');
        const profile = geraGreen?.roastProfile || '061901';
        const dropTemp = geraGreen?.dropTemp || 414;
        
        // Use user's entered weight, not recalculated
        summaryHtml += `<strong>Ethiopia Gera</strong> (~${Math.round(item.weight)}lb roasted):<br>`;
        summaryHtml += `<div style="margin-left: 8px;">`;
        summaryHtml += `- ${batches} batch${batches > 1 ? 'es' : ''} of Ethiopia Gera (${halfBatchWeight}lb lot 58484 + ${halfBatchWeight}lb lot 58479 - profile ${profile} - drop temp ${dropTemp})`;
        summaryHtml += `</div><br><br>`;
        
      } else if (type === 'Single Origin' && recipe) {
        // Other single origins
        const green = greenCoffeeInventory.find(g => g.id === recipe[0].greenCoffeeId);
        const greenWeight = Math.round(item.weight / 0.85);
        const { batches, batchWeight } = calcBatches(greenWeight);
        
        // Use user's entered weight, not recalculated
        summaryHtml += `<strong>${roastedCoffee.name}</strong> (~${Math.round(item.weight)}lb roasted):<br>`;
        summaryHtml += `<div style="margin-left: 8px;">`;
        summaryHtml += `- ${batches} batch${batches > 1 ? 'es' : ''} of ${roastedCoffee.name} (${batchWeight}lb - profile ${green?.roastProfile || '?'} - drop temp ${green?.dropTemp || '?'})`;
        summaryHtml += `</div><br><br>`;
        
      } else {
        // Private Label
        summaryHtml += `<strong>${roastedCoffee.name}</strong>:<br>`;
        summaryHtml += `<div style="margin-left: 8px;">`;
        summaryHtml += `- ${Math.round(item.weight)}lb private label`;
        summaryHtml += `</div><br><br>`;
      }
    }
    
    if (orderItems.length === 0) {
      return res.json({ success: false, message: "I couldn't match those coffees to our inventory. Available: " + roastedCoffeeNames.join(', ') });
    }
    
    summaryHtml += '<div style="color:#888; font-size:12px; margin-bottom:12px;">*Using max 65lb batches minimizes roasting costs</div>';
    summaryHtml += '<div class="response-buttons" style="margin-top: 12px;">';
    summaryHtml += '<button class="action-btn" onclick="confirmDefaultOrder()">Confirm</button>';
    summaryHtml += '<button class="action-btn" onclick="openEditOrderModal()">Edit Order</button>';
    summaryHtml += '<button class="action-btn" onclick="cancelRoastOrder()">Cancel</button>';
    summaryHtml += '</div>';
    
    res.json({
      success: true,
      orderItems: orderItems,
      summary: summaryHtml
    });
    
  } catch (error) {
    console.error('Modify roast order error:', error);
    res.json({ success: false, message: "Error processing modification. Please try again." });
  }
});

// Generate roast order email
app.post('/api/roast-order/generate-email', async (req, res) => {
  const { orderItems } = req.body;
  
  const today = new Date();
  const dateStr = `${today.getMonth() + 1}/${today.getDate()}/${today.getFullYear()}`;
  
  let emailBody = `Hi Shared Team,\n\nHope all is well! Would like to place a toll roast order for the following:\n\n`;
  
  let packagingItems = [];
  
  // Helper to calculate batches
  // For defaults: use full 65lb batches to maximize batch weights
  // For user adjustments: distribute weight evenly across batches (25-65lb range)
  const calcBatches = (totalWeight, forceMax = false) => {
    const batches = Math.ceil(totalWeight / 65);
    if (forceMax) {
      // For default orders, use max 65lb batches
      return { batches, batchWeight: 65 };
    }
    // For user-specified weights, distribute evenly
    if (totalWeight <= 65) {
      return { batches: 1, batchWeight: Math.round(totalWeight) };
    }
    const batchWeight = Math.round(totalWeight / batches);
    return { batches, batchWeight };
  };
  
  orderItems.forEach(item => {
    const roastedCoffee = roastedCoffeeInventory.find(c => c.name === item.name);
    
    if (roastedCoffee && roastedCoffee.name === 'Archives Blend' && roastedCoffee.recipe) {
      // Archives Blend: Brazil batches blended with Yirgacheffe batches
      const totalGreenWeight = Math.round(item.weight / 0.85);
      
      const brazilComp = roastedCoffee.recipe.find(r => r.name.includes('Brazil'));
      const yirgComp = roastedCoffee.recipe.find(r => r.name.includes('Yirgacheffe'));
      
      const brazilGreen = greenCoffeeInventory.find(g => g.id === 'brazil-mogiano');
      const yirgGreen = greenCoffeeInventory.find(g => g.id === 'ethiopia-yirgacheffe');
      
      const brazilWeightNeeded = Math.round(totalGreenWeight * (brazilComp?.percentage || 66.67) / 100);
      const yirgWeightNeeded = Math.round(totalGreenWeight * (yirgComp?.percentage || 33.33) / 100);
      
      const { batches: brazilBatches, batchWeight: brazilBatchWeight } = calcBatches(brazilWeightNeeded);
      const { batches: yirgBatches, batchWeight: yirgBatchWeight } = calcBatches(yirgWeightNeeded);
      
      // Use user's entered weight, not recalculated
      emailBody += `Archives Blend (~${Math.round(item.weight)}lb roasted):\n`;
      emailBody += `- ${brazilBatches} batch${brazilBatches > 1 ? 'es' : ''} of Brazil Mogiano (${brazilBatchWeight}lb - profile ${brazilGreen?.roastProfile || '199503'} - drop temp ${brazilGreen?.dropTemp || 419})\n`;
      emailBody += `blended with\n`;
      emailBody += `- ${yirgBatches} batch${yirgBatches > 1 ? 'es' : ''} of Ethiopia Yirgacheffe (${yirgBatchWeight}lb - profile ${yirgGreen?.roastProfile || '141402'} - drop temp ${yirgGreen?.dropTemp || 415})\n\n`;
      
      packagingItems.push(`~${Math.round(item.weight)}lb roasted Archives Blend`);
      
    } else if (roastedCoffee && roastedCoffee.name === 'Ethiopia Gera') {
      // Ethiopia Gera: Special 50/50 split between two lots per batch
      // Default batch is 65lb, split into 33lb + 33lb (round UP both - exception to max rule)
      const totalGreenWeight = Math.round(item.weight / 0.85);
      const { batches, batchWeight } = calcBatches(totalGreenWeight);
      const halfBatchWeight = Math.ceil(batchWeight / 2); // Round UP to 33lb each
      
      // Find either lot to get roast profile
      const geraGreen = greenCoffeeInventory.find(g => g.id === 'ethiopia-gera-58484' || g.id === 'ethiopia-gera-58479' || g.id === 'ethiopia-gera');
      const profile = geraGreen?.roastProfile || '061901';
      const dropTemp = geraGreen?.dropTemp || 414;
      
      // Use user's entered weight, not recalculated
      emailBody += `Ethiopia Gera (~${Math.round(item.weight)}lb roasted):\n`;
      emailBody += `- ${batches} batch${batches > 1 ? 'es' : ''} of Ethiopia Gera (${halfBatchWeight}lb lot 58484 + ${halfBatchWeight}lb lot 58479 - profile ${profile} - drop temp ${dropTemp})\n\n`;
      
      packagingItems.push(`~${Math.round(item.weight)}lb Ethiopia Gera`);
      
    } else if (roastedCoffee && roastedCoffee.type === 'Single Origin' && roastedCoffee.recipe) {
      // Other Single Origins
      const comp = roastedCoffee.recipe[0];
      const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
      const greenWeight = Math.round(item.weight / 0.85);
      const { batches, batchWeight } = calcBatches(greenWeight);
      
      // Use user's entered weight, not recalculated
      if (greenCoffee) {
        emailBody += `${roastedCoffee.name} (~${Math.round(item.weight)}lb roasted):\n`;
        emailBody += `- ${batches} batch${batches > 1 ? 'es' : ''} of ${roastedCoffee.name} (${batchWeight}lb - profile ${greenCoffee.roastProfile} - drop temp ${greenCoffee.dropTemp})\n\n`;
      }
      
      packagingItems.push(`~${Math.round(item.weight)}lb ${roastedCoffee.name}`);
      
    } else if (roastedCoffee && roastedCoffee.type === 'Private Label') {
      // Private Label - output = input (comes roasted)
      emailBody += `${roastedCoffee.name}:\n`;
      emailBody += `- ${Math.round(item.weight)}lb private label\n\n`;
      packagingItems.push(`${Math.round(item.weight)}lb ${roastedCoffee.name}`);
    }
  });
  
  // Packaging instructions - format list with 'and' before last item
  emailBody += 'Can we have the ';
  if (packagingItems.length === 1) {
    emailBody += packagingItems[0];
  } else if (packagingItems.length === 2) {
    emailBody += packagingItems.join(' and ');
  } else {
    const lastItem = packagingItems.pop();
    emailBody += packagingItems.join(', ') + ', and ' + lastItem;
  }
  emailBody += ' packed in our stamped/labeled bags and shipped using your labels to:\n';
  
  emailBody += '\nRay Park\n869 Estepona Way\nBuena Park, CA 90621\n';
  emailBody += '\nThanks so much!\n\nBest,\nRay';
  
  res.json({
    to: 'samueljhan@gmail.com',
    subject: `AOU Toll Roast Order, ${dateStr}`,
    body: emailBody,
    orderItems: orderItems
  });
});

// Confirm roast order (deduct green inventory, add to en route)
app.post('/api/roast-order/confirm', async (req, res) => {
  // Always fetch fresh inventory from Google Sheets before modifying
  await ensureFreshInventory();
  
  const { orderItems, emailData } = req.body;
  
  // VALIDATION: Check if we have enough green coffee BEFORE making any changes
  const shortages = [];
  const requiredGreen = {}; // Track total needed per green coffee
  
  // Helper to calculate batches (same as email generation)
  const calcBatches = (totalWeight) => {
    const batches = Math.ceil(totalWeight / 65);
    if (totalWeight <= 65) {
      return { batches: 1, batchWeight: Math.round(totalWeight) };
    }
    const batchWeight = Math.round(totalWeight / batches);
    return { batches, batchWeight };
  };
  
  orderItems.forEach(item => {
    const roastedCoffee = roastedCoffeeInventory.find(c => c.name === item.name);
    
    if (roastedCoffee && roastedCoffee.recipe) {
      const totalGreenWeight = Math.round(item.weight / 0.85);
      
      roastedCoffee.recipe.forEach(comp => {
        const compGreenWeight = Math.round(totalGreenWeight * comp.percentage / 100);
        // Use batch calculation to get actual green weight used
        const { batches, batchWeight } = calcBatches(compGreenWeight);
        const actualGreenWeight = batches * batchWeight;
        
        const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
        if (greenCoffee) {
          // Accumulate total required for this green coffee
          if (!requiredGreen[greenCoffee.id]) {
            requiredGreen[greenCoffee.id] = { name: greenCoffee.name, required: 0, available: greenCoffee.weight };
          }
          requiredGreen[greenCoffee.id].required += actualGreenWeight;
        }
      });
    }
  });
  
  // Check for shortages
  Object.values(requiredGreen).forEach(gc => {
    if (gc.required > gc.available) {
      shortages.push({
        name: gc.name,
        required: gc.required,
        available: gc.available,
        shortage: gc.required - gc.available
      });
    }
  });
  
  // If there are shortages, reject the order
  if (shortages.length > 0) {
    const shortageList = shortages.map(s => 
      `${s.name}: need ${s.required}lb but only ${s.available}lb available (short ${s.shortage}lb)`
    ).join('; ');
    
    return res.json({
      success: false,
      error: 'insufficient_inventory',
      message: `Not enough green coffee to complete this order. ${shortageList}. Please reduce the order quantity or choose different coffees. What would you like to do?`,
      shortages
    });
  }
  
  const deductions = [];
  const enRouteItems = [];
  
  // Calculate green coffee deductions using actual batch weights
  orderItems.forEach(item => {
    const roastedCoffee = roastedCoffeeInventory.find(c => c.name === item.name);
    
    if (roastedCoffee && roastedCoffee.recipe) {
      const totalGreenWeight = Math.round(item.weight / 0.85);
      
      // Calculate green coffee needed for each component using batch logic
      roastedCoffee.recipe.forEach(comp => {
        const compGreenWeight = Math.round(totalGreenWeight * comp.percentage / 100);
        // Use batch calculation to get actual green weight used
        const { batches, batchWeight } = calcBatches(compGreenWeight);
        const actualGreenWeight = batches * batchWeight;
        
        const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
        if (greenCoffee) {
          greenCoffee.weight -= actualGreenWeight;
          deductions.push({
            name: greenCoffee.name,
            deducted: actualGreenWeight,
            remaining: greenCoffee.weight
          });
        }
      });
    }
    
    // Add to en route inventory with mm/dd/yy date format
    const now = new Date();
    enRouteItems.push({
      id: `enroute-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      name: item.name,
      weight: item.weight,
      type: roastedCoffee ? roastedCoffee.type : 'Unknown',
      recipe: roastedCoffee ? roastedCoffee.recipe : null,
      trackingNumber: '',
      dateOrdered: formatDateMMDDYY(now)
    });
  });
  
  // Add items to en route inventory
  enRouteCoffeeInventory.push(...enRouteItems);
  
  // Create Gmail draft if connected
  let draftCreated = false;
  if (userTokens && emailData) {
    try {
      oauth2Client.setCredentials(userTokens);
      const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
      
      const emailContent = [
        `To: ${emailData.to}`,
        `Subject: ${emailData.subject}`,
        'Content-Type: text/plain; charset=utf-8',
        '',
        emailData.body
      ].join('\n');
      
      const encodedEmail = Buffer.from(emailContent)
        .toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/, '');
      
      await gmail.users.drafts.create({
        userId: 'me',
        requestBody: {
          message: { raw: encodedEmail }
        }
      });
      
      draftCreated = true;
      console.log('📝 Roast order draft created');
    } catch (error) {
      console.error('Failed to create draft:', error);
    }
  }
  
  // Sync inventory to Sheets before responding
  await syncInventoryToSheets();
  
  res.json({
    success: true,
    deductions,
    enRouteItems,
    draftCreated,
    message: `Order confirmed! ${deductions.length > 0 ? 'Green coffee inventory updated.' : ''} ${enRouteItems.length} item(s) added to en route.${draftCreated ? ' Email draft created in Gmail.' : ''} What else can I help you with?`
  });
});

// ============ Start Server ============

server.listen(PORT, '0.0.0.0', () => {
  console.log(`📦 Mise Flow running on port ${PORT}`);
  console.log(`🔐 Authentication required`);
  console.log(`🎤 AWS Transcribe enabled`);
  console.log(`🧠 ChatGPT (OpenAI) for interpretation`);
  console.log(`✨ Gemini for Google Workspace operations`);
  console.log(`📧 Gmail integration ready`);
  console.log(`📊 Google Sheets integration ready`);
});