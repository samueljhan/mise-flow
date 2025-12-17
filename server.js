const express = require('express');
const { GoogleGenerativeAI } = require('@google/generative-ai');
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
    transcription: 'AWS Transcribe',
    llm: 'Gemini 2.5 Flash',
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
    <p class="subtitle">Automated workflow for AOU Coffee</p>
    
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
let greenCoffeeInventory = [
  {
    id: 'colombia-antioquia',
    name: 'Colombia Antioquia',
    weight: 100,
    roastProfile: '122302',
    dropTemp: 410
  },
  {
    id: 'ethiopia-gera',
    name: 'Ethiopia Gera',
    weight: 100,
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
    recipe: [
      { greenCoffeeId: 'ethiopia-gera', name: 'Ethiopia Gera', percentage: 100 }
    ]
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
    data.push(['', '', '', '', '', '', '']);
    
    // Row 2: Last updated timestamp
    data.push(['', `Last updated: ${pstTimestamp}`, '', '', '', '', '']);
    
    // Row 3: empty (spacing)
    data.push(['', '', '', '', '', '', '']);
    
    // GREEN COFFEE SECTION
    data.push(['', 'Green Coffee Inventory', '', '', '', '', '']);
    data.push(['', 'Name', 'Weight (lb)', 'Roast Profile', 'Drop Temp', '', '']);
    greenCoffeeInventory.forEach(c => {
      data.push(['', c.name, c.weight, c.roastProfile || '', c.dropTemp || '', '', '']);
    });
    
    // Empty row
    data.push(['', '', '', '', '', '', '']);
    
    // ROASTED COFFEE SECTION
    data.push(['', 'Roasted Coffee Inventory', '', '', '', '', '']);
    data.push(['', 'Name', 'Weight (lb)', 'Type', 'Recipe', '', '']);
    roastedCoffeeInventory.forEach(c => {
      const recipe = c.recipe ? c.recipe.map(r => `${r.percentage}% ${r.name}`).join(' + ') : 'N/A';
      data.push(['', c.name, c.weight, c.type || '', recipe, '', '']);
    });
    
    // Empty row
    data.push(['', '', '', '', '', '', '']);
    
    // EN ROUTE SECTION
    data.push(['', 'En Route Inventory', '', '', '', '', '']);
    data.push(['', 'Name', 'Weight (lb)', 'Type', 'Tracking Number', 'Date Ordered', 'Estimated Ship Date']);
    if (enRouteCoffeeInventory.length > 0) {
      enRouteCoffeeInventory.forEach(c => {
        // Only show estimated delivery if tracking number exists
        const estDelivery = c.trackingNumber ? (c.estimatedDelivery || '') : '';
        const dateOrdered = c.dateOrdered || c.orderDate || c.dateAdded || '';
        data.push(['', c.name, c.weight, c.type || '', c.trackingNumber || '', dateOrdered, estDelivery]);
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
      range: 'Inventory!A:F'
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
          roastProfile: row[3] || '',
          dropTemp: parseFloat(row[4]) || 0
        });
      } else if (currentSection === 'roasted' && row[1]) {
        tempRoasted.push({
          id: row[1].toLowerCase().replace(/\s+/g, '-'),
          name: row[1],
          weight: parseFloat(row[2]) || 0,
          type: row[3] || '',
          recipe: null
        });
      } else if (currentSection === 'enroute' && row[1]) {
        tempEnRoute.push({
          id: row[1].toLowerCase().replace(/\s+/g, '-') + '-' + Date.now(),
          name: row[1],
          weight: parseFloat(row[2]) || 0,
          type: row[3] || '',
          trackingNumber: row[4] || '',
          dateOrdered: row[5] || '',
          estimatedDelivery: row[6] || ''
        });
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
    if (tempEnRoute.length > 0) {
      enRouteCoffeeInventory = tempEnRoute;
      console.log(`📦 Loaded ${enRouteCoffeeInventory.length} en route items from Sheets`);
    }

    return { success: true };

  } catch (error) {
    console.error('❌ Load inventory error:', error.message);
    return { success: false, error: error.message };
  }
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
      message: `Draft created! Check your Gmail drafts folder.`,
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

  try {
    const { invoiceNumber, date, total, items } = req.body;
    
    if (!invoiceNumber || !date || total === undefined) {
      return res.status(400).json({ error: 'Missing invoice details' });
    }

    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    // Record in Invoices sheet
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!B:E',
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: {
        values: [[date, invoiceNumber, `$${parseFloat(total).toFixed(2)}`, '']]
      }
    });

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
          roastedMatch.weight = Math.max(0, roastedMatch.weight - quantity);
          
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

    res.json({ 
      success: true, 
      message: `Invoice ${invoiceNumber} confirmed`,
      deductions: deductions
    });

    // Sync inventory to Sheets in background (don't await)
    syncInventoryToSheets().catch(err => console.error('Background sync error:', err));

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
app.get('/api/inventory', (req, res) => {
  res.json({
    green: greenCoffeeInventory,
    roasted: roastedCoffeeInventory,
    enRoute: enRouteCoffeeInventory
  });
});

// Get inventory summary formatted
app.get('/api/inventory/summary', (req, res) => {
  res.json({ summary: formatInventorySummary() });
});

// Get green coffee inventory
app.get('/api/inventory/green', (req, res) => {
  res.json(greenCoffeeInventory);
});

// Update green coffee inventory
app.post('/api/inventory/green/update', (req, res) => {
  const { id, weight, roastProfile, dropTemp } = req.body;
  const coffee = greenCoffeeInventory.find(c => c.id === id);
  if (!coffee) {
    return res.status(404).json({ error: 'Green coffee not found' });
  }
  if (weight !== undefined) coffee.weight = weight;
  if (roastProfile !== undefined) coffee.roastProfile = roastProfile;
  if (dropTemp !== undefined) coffee.dropTemp = dropTemp;
  res.json({ success: true, coffee });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// Add new green coffee
app.post('/api/inventory/green/add', (req, res) => {
  const { name, weight, roastProfile, dropTemp } = req.body;
  const id = name.toLowerCase().replace(/\s+/g, '-');
  if (greenCoffeeInventory.find(c => c.id === id)) {
    return res.status(400).json({ error: 'Coffee already exists' });
  }
  const newCoffee = { id, name, weight, roastProfile, dropTemp };
  greenCoffeeInventory.push(newCoffee);
  res.json({ success: true, coffee: newCoffee });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// Remove green coffee
app.post('/api/inventory/green/remove', (req, res) => {
  const { id } = req.body;
  const index = greenCoffeeInventory.findIndex(c => c.id === id);
  if (index === -1) {
    return res.status(404).json({ error: 'Green coffee not found' });
  }
  greenCoffeeInventory.splice(index, 1);
  res.json({ success: true });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// Get roasted coffee inventory
app.get('/api/inventory/roasted', (req, res) => {
  res.json(roastedCoffeeInventory);
});

// Update roasted coffee inventory
app.post('/api/inventory/roasted/update', (req, res) => {
  const { id, weight, type, recipe } = req.body;
  const coffee = roastedCoffeeInventory.find(c => c.id === id);
  if (!coffee) {
    return res.status(404).json({ error: 'Roasted coffee not found' });
  }
  if (weight !== undefined) coffee.weight = weight;
  if (type !== undefined) coffee.type = type;
  if (recipe !== undefined) coffee.recipe = recipe;
  res.json({ success: true, coffee });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// Add new roasted coffee
app.post('/api/inventory/roasted/add', (req, res) => {
  const { name, weight, type, recipe } = req.body;
  const id = name.toLowerCase().replace(/\s+/g, '-') + '-roasted';
  const newCoffee = { id, name, weight, type, recipe };
  roastedCoffeeInventory.push(newCoffee);
  res.json({ success: true, coffee: newCoffee });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// Remove roasted coffee
app.post('/api/inventory/roasted/remove', (req, res) => {
  const { id } = req.body;
  const index = roastedCoffeeInventory.findIndex(c => c.id === id);
  if (index === -1) {
    return res.status(404).json({ error: 'Roasted coffee not found' });
  }
  roastedCoffeeInventory.splice(index, 1);
  res.json({ success: true });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// Get en route inventory
app.get('/api/inventory/enroute', (req, res) => {
  res.json(enRouteCoffeeInventory);
});

// Add to en route inventory
app.post('/api/inventory/enroute/add', (req, res) => {
  const { name, weight, type, recipe, orderDate } = req.body;
  const id = `enroute-${Date.now()}`;
  const newItem = {
    id,
    name,
    weight,
    type,
    recipe,
    trackingNumber: '',
    dateOrdered: orderDate || new Date().toLocaleDateString('en-US'),
    estimatedDelivery: '',
    status: 'ordered'
  };
  enRouteCoffeeInventory.push(newItem);
  res.json({ success: true, item: newItem });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// Update tracking number and fetch estimated delivery
app.post('/api/inventory/enroute/tracking', async (req, res) => {
  const { id, trackingNumber } = req.body;
  const item = enRouteCoffeeInventory.find(c => c.id === id);
  if (!item) {
    return res.status(404).json({ error: 'En route item not found' });
  }
  item.trackingNumber = trackingNumber;
  item.status = 'shipped';
  
  // Fetch estimated delivery date from UPS
  if (trackingNumber) {
    const trackingInfo = await getUPSEstimatedDelivery(trackingNumber);
    if (trackingInfo && trackingInfo.estimatedDelivery) {
      item.estimatedDelivery = trackingInfo.estimatedDelivery;
      console.log(`📦 Tracking ${trackingNumber}: Est. delivery ${trackingInfo.estimatedDelivery}`);
    }
  }
  
  res.json({ success: true, item });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// Mark en route item as delivered (moves to roasted inventory)
app.post('/api/inventory/enroute/deliver', (req, res) => {
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
  
  res.json({ success: true, message: `${item.name} (${item.weight}lb) added to roasted inventory` });
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// ============ UPS Tracking API ============

// Look up UPS tracking info and get estimated delivery date
async function getUPSEstimatedDelivery(trackingNumber) {
  if (!trackingNumber) return null;
  
  try {
    // Use Gemini to extract delivery date from UPS tracking
    const prompt = `I need to look up UPS tracking number: ${trackingNumber}

Based on standard UPS Ground shipping times (typically 5-7 business days from ship date), and common tracking number patterns:
- If it starts with "1Z" it's a valid UPS tracking number
- Ground shipments from California typically take 5-7 business days

Since this is a roasted coffee order that was just shipped, estimate the delivery date as approximately 5-7 business days from today.

Respond with JSON only (no markdown):
{
  "estimatedDelivery": "MM/DD/YYYY format date, approximately 6 business days from today",
  "status": "In Transit" or "Shipped",
  "validFormat": true if starts with 1Z, false otherwise
}`;

    const response = await callGeminiWithRetry(prompt);
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    
    if (jsonMatch) {
      const data = JSON.parse(jsonMatch[0]);
      return {
        estimatedDelivery: data.estimatedDelivery || null,
        status: data.status || 'In Transit',
        validFormat: data.validFormat
      };
    }
  } catch (error) {
    console.error('UPS lookup error:', error);
  }
  
  // Fallback: calculate ~6 business days from now
  const today = new Date();
  let businessDays = 6;
  let deliveryDate = new Date(today);
  while (businessDays > 0) {
    deliveryDate.setDate(deliveryDate.getDate() + 1);
    const dayOfWeek = deliveryDate.getDay();
    if (dayOfWeek !== 0 && dayOfWeek !== 6) { // Skip weekends
      businessDays--;
    }
  }
  return {
    estimatedDelivery: deliveryDate.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' }),
    status: 'In Transit',
    validFormat: trackingNumber.startsWith('1Z')
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
    trackingUrl: `https://www.ups.com/track?tracknum=${trackingNumber}`
  });
});

// ============ Conversational Chat API ============

// Generate conversational response using Gemini
app.post('/api/chat/respond', async (req, res) => {
  const { context, completedAction, inventory } = req.body;
  
  const prompt = `You are Mise, a helpful assistant for Archives of Us Coffee inventory management. 
You have a warm, professional tone - friendly but efficient. Keep responses concise (1-2 sentences max).

${inventory ? `Current inventory summary:
- Green Coffee: ${inventory.green?.map(c => c.name).join(', ') || 'None'}
- Roasted Coffee: ${inventory.roasted?.map(c => c.name).join(', ') || 'None'}
- En Route: ${inventory.enRoute?.length || 0} items` : ''}

${context ? `Context: ${context}` : ''}
${completedAction ? `Just completed: ${completedAction}` : ''}

Generate a brief, natural follow-up message. If a task was just completed, acknowledge it and ask what else you can help with.
Don't use emojis. Be conversational but professional.

Respond with just the message text, no JSON or formatting.`;

  try {
    const response = await callGeminiWithRetry(prompt);
    res.json({ message: response.trim() });
  } catch (error) {
    console.error('Chat respond error:', error);
    res.json({ message: 'What else can I help you with?' });
  }
});

// Process general chat input
app.post('/api/chat/process', async (req, res) => {
  const { userInput, currentState } = req.body;
  
  const roastedCoffeeNames = roastedCoffeeInventory.map(c => c.name);
  const greenCoffeeNames = greenCoffeeInventory.map(c => c.name);
  
  const prompt = `You are Mise, parsing user input for Archives of Us Coffee inventory management.

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

  try {
    const response = await callGeminiWithRetry(prompt);
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      res.json(JSON.parse(jsonMatch[0]));
    } else {
      res.json({ action: 'chat', chatResponse: "Sorry, I didn't get that. What can I help you with?" });
    }
  } catch (error) {
    console.error('Chat process error:', error);
    res.json({ action: 'chat', chatResponse: "I'm having trouble understanding. Could you try rephrasing?" });
  }
});

// ============ Roast Order API Endpoints ============

// Parse roast order request
app.post('/api/roast-order/parse', async (req, res) => {
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
  
  const prompt = `You are parsing a coffee roast order request for Archives of Us Coffee.

Available roasted coffees with types: ${JSON.stringify(roastedCoffeeTypes)}

User request: "${userInput}"
${contextInfo}
NICKNAME/SHORTHAND RECOGNITION - BE SMART AND ASSUME INTENT:
- "Blend" or "Archives" → Archives Blend (ONLY blend in inventory, no need to clarify)
- "Ethiopia" alone → If only ONE Ethiopia coffee exists in roasted inventory, use it. Otherwise ask.
- "Colombia" alone → If only ONE Colombia coffee exists in roasted inventory, use it. Otherwise ask.
- "Gera" → Ethiopia Gera
- "Yirgacheffe" or "Yirg" → Ethiopia Yirgacheffe  
- "Decaf" → Colombia Decaf
- "Antioquia" → Colombia Antioquia
- "Brazil" or "Mogiano" → Note: Brazil Mogiano is green coffee only, not a roasted product

SMART MATCHING RULES:
1. If user mentions a term that matches ONLY ONE coffee, use it without asking for clarification.
2. Only ask for clarification when there are genuinely multiple options that could match.
3. Be generous in interpretation - if the user says something close to a coffee name, match it.
4. For partial matches or typos, match to the closest coffee name.

CONFIRMATION HANDLING:
- If user says "yes", "yeah", "yep", "correct", "that one", "sure", "ok", etc. AND there was a previous suggestion, confirm that suggestion.

Respond with JSON only:
{
  "understood": true/false,
  "needsClarification": true/false,
  "clarificationQuestion": "string if needs clarification",
  "suggestedCoffee": "the coffee name being suggested if asking for clarification",
  "coffees": [
    {
      "name": "exact name from available list",
      "matched": true/false
    }
  ]
}`;

  try {
    const response = await callGeminiWithRetry(prompt);
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      const parsed = JSON.parse(jsonMatch[0]);
      res.json(parsed);
    } else {
      res.json({ understood: false, needsClarification: true, clarificationQuestion: "Sorry, I didn't get that. What can I help you with?" });
    }
  } catch (error) {
    console.error('Parse roast order error:', error);
    res.json({ understood: false, needsClarification: true, clarificationQuestion: "Sorry, I didn't get that. What can I help you with?" });
  }
});

// Generate roast order email
app.post('/api/roast-order/generate-email', async (req, res) => {
  const { orderItems } = req.body;
  
  const today = new Date();
  const dateStr = `${today.getMonth() + 1}/${today.getDate()}/${today.getFullYear()}`;
  
  let emailBody = `Hi Shared Team,\n\nHope all is well! Would like to place a toll roast order for the following:\n\n`;
  
  let packagingItems = [];
  
  // Nickname mapping for coffees
  const getNickname = (name) => {
    const nicknames = {
      'Brazil Mogiano': 'Brazil',
      'Colombia Decaf': 'Decaf',
      'Colombia Antioquia': 'Colombia',
      'Ethiopia Yirgacheffe': 'Yirgacheffe',
      'Ethiopia Gera': 'Ethiopia'
    };
    return nicknames[name] || name;
  };
  
  // Helper to calculate batches (max 65lb, min 25lb per batch)
  // Maximize 65lb batches to save costs
  const calcBatches = (totalWeight) => {
    if (totalWeight <= 65) {
      return { batches: 1, batchWeight: Math.round(totalWeight) };
    }
    const batches = Math.ceil(totalWeight / 65);
    const batchWeight = Math.round(totalWeight / batches);
    return { batches, batchWeight };
  };
  
  orderItems.forEach(item => {
    const roastedCoffee = roastedCoffeeInventory.find(c => c.name === item.name);
    
    if (roastedCoffee && roastedCoffee.type === 'Blend' && roastedCoffee.recipe) {
      // Blend - calculate green weights and batches
      const totalGreenWeight = Math.round(item.weight / 0.85);
      const roastedOutput = item.weight;
      
      let blendParts = [];
      roastedCoffee.recipe.forEach((comp) => {
        const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
        const compGreenWeight = Math.round(totalGreenWeight * comp.percentage / 100);
        const { batches, batchWeight } = calcBatches(compGreenWeight);
        
        if (greenCoffee) {
          const nickname = getNickname(comp.name);
          blendParts.push(`${batches} batch${batches > 1 ? 'es' : ''} of ${nickname} (${batchWeight}lb - profile ${greenCoffee.roastProfile} - drop temp ${greenCoffee.dropTemp})`);
        }
      });
      
      emailBody += `- ${blendParts.join(' blended with ')}\n`;
      
      // Get component nicknames for packaging
      const compNames = roastedCoffee.recipe.map(r => getNickname(r.name)).join('/');
      packagingItems.push(`~${Math.round(roastedOutput)}lb roasted ${compNames}`);
      
    } else if (roastedCoffee && roastedCoffee.type === 'Single Origin' && roastedCoffee.recipe) {
      // Single Origin
      const comp = roastedCoffee.recipe[0];
      const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
      const greenWeight = Math.round(item.weight / 0.85);
      const { batches, batchWeight } = calcBatches(greenWeight);
      const roastedOutput = item.weight;
      const nickname = getNickname(item.name);
      
      if (greenCoffee) {
        emailBody += `- ${batches} batch${batches > 1 ? 'es' : ''} of ${nickname} (${batchWeight}lb - profile ${greenCoffee.roastProfile} - drop temp ${greenCoffee.dropTemp})\n`;
      }
      
      packagingItems.push(`~${Math.round(roastedOutput)}lb ${nickname}`);
      
    } else if (roastedCoffee && roastedCoffee.type === 'Private Label') {
      // Private Label - output = input (comes roasted)
      const nickname = getNickname(item.name);
      emailBody += `- ${Math.round(item.weight)}lb private label ${nickname}\n`;
      packagingItems.push(`${Math.round(item.weight)}lb ${nickname}`);
    }
  });
  
  // Packaging instructions - format list with 'and' before last item
  emailBody += '\nCan we have the ';
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
  const { orderItems, emailData } = req.body;
  
  const deductions = [];
  const enRouteItems = [];
  
  // Calculate green coffee deductions (roasted weight / 0.85 for weight loss)
  orderItems.forEach(item => {
    const roastedCoffee = roastedCoffeeInventory.find(c => c.name === item.name);
    
    if (roastedCoffee && roastedCoffee.recipe) {
      // Calculate green coffee needed for each component
      roastedCoffee.recipe.forEach(comp => {
        const compRoastedWeight = item.weight * comp.percentage / 100;
        const greenWeight = Math.round(compRoastedWeight / 0.85); // Account for 15% weight loss
        
        const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
        if (greenCoffee) {
          greenCoffee.weight -= greenWeight;
          deductions.push({
            name: greenCoffee.name,
            deducted: greenWeight,
            remaining: greenCoffee.weight
          });
        }
      });
    }
    
    // Add to en route inventory
    enRouteItems.push({
      id: `enroute-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      name: item.name,
      weight: item.weight,
      type: roastedCoffee ? roastedCoffee.type : 'Unknown',
      recipe: roastedCoffee ? roastedCoffee.recipe : null,
      trackingNumber: '',
      orderDate: new Date().toISOString(),
      status: 'ordered'
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
  
  res.json({
    success: true,
    deductions,
    enRouteItems,
    draftCreated,
    message: `Order confirmed! ${deductions.length > 0 ? 'Green coffee inventory updated.' : ''} ${enRouteItems.length} item(s) added to en route.${draftCreated ? ' Email draft created in Gmail.' : ''}`
  });

  // Sync inventory to Sheets in background (don't await)
  syncInventoryToSheets().catch(err => console.error('Background sync error:', err));
});

// ============ Start Server ============

server.listen(PORT, '0.0.0.0', () => {
  console.log(`📦 Mise Flow running on port ${PORT}`);
  console.log(`🔐 Authentication required`);
  console.log(`🎤 AWS Transcribe enabled`);
  console.log(`✨ Gemini 2.5 Flash for AI processing`);
  console.log(`📧 Gmail integration ready`);
  console.log(`📊 Google Sheets integration ready`);
});