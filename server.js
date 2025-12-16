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

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

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

// ============ Google OAuth Routes ============

app.get('/auth/google', (req, res) => {
  const scopes = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.compose',
    'https://www.googleapis.com/auth/spreadsheets'
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
    
    console.log('✅ Google OAuth successful (Gmail + Sheets)');
    
    // Log refresh token so it can be saved to environment variables
    if (tokens.refresh_token) {
      console.log('=== SAVE THIS REFRESH TOKEN TO RAILWAY ===');
      console.log(tokens.refresh_token);
      console.log('==========================================');
      console.log('Add to Railway Variables as: GOOGLE_REFRESH_TOKEN');
    }
    
    res.redirect('/?google=connected');
  } catch (error) {
    console.error('OAuth error:', error);
    res.redirect('/?google=error');
  }
});

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
      // Token refresh failed, clear tokens
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
  res.json({ success: true });
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
    const { invoiceNumber, date, total } = req.body;
    
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

    res.json({ success: true, message: `Invoice ${invoiceNumber} confirmed` });

  } catch (error) {
    console.error('Invoice confirmation error:', error);
    res.status(500).json({ error: 'Failed to confirm invoice', details: error.message });
  }
});


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
});

// Add new roasted coffee
app.post('/api/inventory/roasted/add', (req, res) => {
  const { name, weight, type, recipe } = req.body;
  const id = name.toLowerCase().replace(/\s+/g, '-') + '-roasted';
  const newCoffee = { id, name, weight, type, recipe };
  roastedCoffeeInventory.push(newCoffee);
  res.json({ success: true, coffee: newCoffee });
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
    orderDate: orderDate || new Date().toISOString(),
    status: 'ordered'
  };
  enRouteCoffeeInventory.push(newItem);
  res.json({ success: true, item: newItem });
});

// Update tracking number
app.post('/api/inventory/enroute/tracking', (req, res) => {
  const { id, trackingNumber } = req.body;
  const item = enRouteCoffeeInventory.find(c => c.id === id);
  if (!item) {
    return res.status(404).json({ error: 'En route item not found' });
  }
  item.trackingNumber = trackingNumber;
  item.status = 'shipped';
  res.json({ success: true, item });
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
});

// ============ UPS Tracking API ============

// Look up UPS tracking info
app.post('/api/ups/track', async (req, res) => {
  const { trackingNumber } = req.body;
  
  if (!trackingNumber) {
    return res.json({ error: 'No tracking number provided' });
  }
  
  // Use Gemini to simulate tracking lookup (in production, use actual UPS API)
  // For now, provide a helpful response with link to UPS
  try {
    const prompt = `A user wants to track a UPS package with tracking number: ${trackingNumber}

Since I don't have direct access to UPS tracking API, provide a helpful response. 
If the tracking number looks valid (1Z followed by alphanumeric characters, or starts with certain patterns), 
say it appears to be a valid UPS tracking number.

Respond with JSON only:
{
  "status": "In Transit" or "Shipped" or "Unknown",
  "validFormat": true/false,
  "message": "brief message about the tracking number"
}`;

    const response = await callGeminiWithRetry(prompt);
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    
    if (jsonMatch) {
      const data = JSON.parse(jsonMatch[0]);
      res.json({
        trackingNumber,
        status: data.status || 'Check UPS.com for status',
        validFormat: data.validFormat,
        trackingUrl: `https://www.ups.com/track?tracknum=${trackingNumber}`
      });
    } else {
      res.json({
        trackingNumber,
        status: 'Check UPS.com for status',
        trackingUrl: `https://www.ups.com/track?tracknum=${trackingNumber}`
      });
    }
  } catch (error) {
    console.error('UPS tracking error:', error);
    res.json({
      trackingNumber,
      status: 'Unable to verify',
      trackingUrl: `https://www.ups.com/track?tracknum=${trackingNumber}`
    });
  }
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
});

// ============ Health Check ============

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'Mise Flow API',
    transcription: 'AWS Transcribe',
    llm: 'Gemini 2.5 Flash',
    google: userTokens ? 'connected' : 'not connected'
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`📦 Mise Flow running on port ${PORT}`);
  console.log(`🎤 AWS Transcribe enabled`);
  console.log(`✨ Gemini 2.5 Flash for AI processing`);
  console.log(`📧 Gmail integration ready`);
  console.log(`📊 Google Sheets integration ready`);
});