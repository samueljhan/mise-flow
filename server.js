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
  console.log('✓ Google account auto-connected from saved refresh token');
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

app.get('/api/google/status', (req, res) => {
  res.json({ 
    connected: !!userTokens,
    services: userTokens ? ['Gmail', 'Sheets'] : []
  });
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

    // Try pattern matching first (fast, no API call)
    let customer = null, quantity = null, product = null;
    
    // Common patterns
    const patterns = [
      /(?:for\s+)?(\w+(?:'s)?(?:\s+\w+)?)\s*[,.]?\s*(\d+)\s*(?:lbs?|pounds?|lb)?\s*(?:of\s+)?(.+)/i,
      /^(\w+(?:'s)?(?:\s+\w+)?)\s+(\d+)\s*(?:lbs?|pounds?|lb)?\s*(.+)$/i,
      /(.+?)\s+(\d+)\s*(?:lbs?|pounds?|lb)?\s*(?:for\s+)?(\w+(?:'s)?(?:\s+\w+)?)$/i
    ];
    
    const knownProducts = ['Archives Blend', 'Ethiopia Gera Natural', 'Colombia Excelso', 'Colombia Decaf'];
    const productAliases = {
      'archives': 'Archives Blend', 'archive': 'Archives Blend', 'house': 'Archives Blend', 'blend': 'Archives Blend',
      'ethiopia': 'Ethiopia Gera Natural', 'ethiopian': 'Ethiopia Gera Natural',
      'colombia': 'Colombia Excelso', 'colombian': 'Colombia Excelso',
      'decaf': 'Colombia Decaf', 'decaffeinated': 'Colombia Decaf'
    };
    
    for (const pattern of patterns) {
      const match = details.match(pattern);
      if (match) {
        const [_, part1, qty, part3] = match;
        quantity = parseInt(qty);
        
        // Figure out which is customer and which is product
        const part1Lower = part1.toLowerCase().trim();
        const part3Lower = part3.toLowerCase().trim();
        
        // Check if part1 is a customer
        const customerFromPart1 = getKnownCustomers().find(c => 
          c.toLowerCase() === part1Lower ||
          part1Lower.includes(c.toLowerCase()) ||
          c.toLowerCase().includes(part1Lower)
        );
        
        // Check if part3 contains a product
        const productFromPart3 = knownProducts.find(p => part3Lower.includes(p.toLowerCase().split(' ')[0])) ||
          Object.entries(productAliases).find(([alias]) => part3Lower.includes(alias))?.[1];
        
        if (customerFromPart1 && productFromPart3) {
          customer = customerFromPart1;
          product = productFromPart3;
          console.log(`✅ Pattern matched: customer="${customer}", quantity=${quantity}, product="${product}"`);
          break;
        }
        
        // Try reverse (product first, customer last)
        const productFromPart1 = knownProducts.find(p => part1Lower.includes(p.toLowerCase().split(' ')[0])) ||
          Object.entries(productAliases).find(([alias]) => part1Lower.includes(alias))?.[1];
        const customerFromPart3 = getKnownCustomers().find(c => 
          c.toLowerCase() === part3Lower ||
          part3Lower.includes(c.toLowerCase()) ||
          c.toLowerCase().includes(part3Lower)
        );
        
        if (productFromPart1 && customerFromPart3) {
          customer = customerFromPart3;
          product = productFromPart1;
          console.log(`✅ Pattern matched (reversed): customer="${customer}", quantity=${quantity}, product="${product}"`);
          break;
        }
        
        // Partial match - keep trying other patterns
        if (customerFromPart1) customer = customerFromPart1;
        if (productFromPart3) product = productFromPart3;
        if (productFromPart1) product = productFromPart1;
        if (customerFromPart3) customer = customerFromPart3;
      }
    }
    
    // If pattern matching didn't get everything, try Gemini
    if (!customer || !quantity || !product) {
      console.log(`⚡ Pattern matching incomplete, trying Gemini...`);
      
      const parsePrompt = `Parse this invoice request. Input: "${details}"
KNOWN CUSTOMERS: ${getKnownCustomers().join(', ')}
KNOWN PRODUCTS: Archives Blend, Ethiopia Gera Natural, Colombia Excelso, Colombia Decaf

Respond ONLY with JSON: {"customer": "name", "quantity": number, "product": "name"}`;

      try {
        const parseText = await callGeminiWithRetry(parsePrompt, { maxRetries: 2 });
        console.log(`🤖 Gemini parse: ${parseText}`);
        
        const cleanJson = parseText.replace(/```json\n?|\n?```/g, '').trim();
        const parsed = JSON.parse(cleanJson);
        
        customer = customer || parsed.customer;
        quantity = quantity || parseInt(parsed.quantity);
        product = product || parsed.product;
      } catch (parseError) {
        if (parseError.message === 'RATE_LIMITED') {
          console.log('⚠️ Rate limited during parsing');
          if (!customer || !quantity || !product) {
            return res.status(429).json({ 
              error: 'System is busy. Please try again in a moment or use format: "CustomerName 100 lbs Product"' 
            });
          }
        } else {
          console.error('⚠️ Gemini parsing failed:', parseError.message);
        }
      }
    }
    
    if (!customer || !quantity || !product) {
      return res.status(400).json({ error: 'Could not parse invoice details. Please use format: "CustomerName 100 lbs Product"' });
    }
    
    // Match customer to known list
    const normalizedCustomer = getKnownCustomers().find(c => 
      c.toLowerCase() === customer.toLowerCase() ||
      customer.toLowerCase().includes(c.toLowerCase()) ||
      c.toLowerCase().includes(customer.toLowerCase())
    );
    if (normalizedCustomer) customer = normalizedCustomer;

    console.log(`📝 Generating invoice for: ${customer}, ${quantity} lbs ${product}`);

    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    // Step 1: Get pricing from Wholesale Pricing sheet (entire sheet including At-Cost)
    const pricingResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Wholesale Pricing!A:H'  // Extended to column H for At-Cost pricing
    });
    
    const pricingRows = pricingResponse.data.values || [];
    
    // Extract all table headers (customers) and products from the sheet
    const tableHeaders = [];
    const allProducts = [];
    
    for (let i = 0; i < pricingRows.length; i++) {
      const row = pricingRows[i];
      const cellB = (row[1] || '').toString().trim();
      
      if (cellB.toLowerCase().includes('wholesale') || cellB.toLowerCase().includes('at-cost')) {
        tableHeaders.push({ name: cellB, row: i });
      } else if (cellB && cellB.toLowerCase() !== 'coffee' && !cellB.toLowerCase().includes('price')) {
        allProducts.push(cellB);
      }
    }
    
    // Remove duplicate products
    const uniqueProducts = [...new Set(allProducts)];
    
    console.log(`📋 Available tables: ${tableHeaders.map(t => t.name).join(', ')}`);
    console.log(`📋 Available products: ${uniqueProducts.join(', ')}`);
    console.log(`📋 Known customers: ${getKnownCustomers().join(', ')}`);
    
    // Prepare Gemini matching prompt (used if direct matching fails)
    const matchPrompt = `You are a matching assistant. Match the user input to the closest option from the available lists.

Known customers:
${getKnownCustomers().map(c => `- ${c}`).join('\n')}

Available products:
${uniqueProducts.map(p => `- ${p}`).join('\n')}

User entered:
- Customer: "${customer}"
- Product: "${product}"

Respond ONLY with valid JSON (no markdown, no explanation):
{
  "matchedCustomer": "exact name from known customers list or null if no close match",
  "matchedProduct": "exact name from product list or null if no close match",
  "customerConfidence": "high/medium/low/none",
  "productConfidence": "high/medium/low"
}

Rules:
- Match even with typos, abbreviations, or partial names
- "CED" should match "CED"
- "Dex" or "deks" should match "Dex"
- "Archives" or "AOU" should match "Archives of Us"
- "junia" or "juna" should match "Junia"
- "Archives" or "archive blend" for PRODUCT should match "Archives Blend"
- "Ethiopia" should match "Ethiopia Gera Natural"
- If the customer doesn't closely match any known customer, set matchedCustomer to null and customerConfidence to "none"`;

    let matchedCustomer = null;
    let matchedProduct = null;
    let customerConfidence = 'none';
    
    // First, check for direct customer match (case-insensitive)
    const directCustomerMatch = getKnownCustomers().find(c => 
      c.toLowerCase() === customer.toLowerCase() ||
      customer.toLowerCase().includes(c.toLowerCase()) ||
      c.toLowerCase().includes(customer.toLowerCase())
    );
    
    if (directCustomerMatch) {
      matchedCustomer = directCustomerMatch;
      customerConfidence = 'high';
      console.log(`✅ Direct customer match: "${matchedCustomer}"`);
    }
    
    // Check for direct product match
    const directProductMatch = uniqueProducts.find(p =>
      p.toLowerCase() === product.toLowerCase() ||
      product.toLowerCase().includes(p.toLowerCase().split(' ')[0]) ||
      p.toLowerCase().includes(product.toLowerCase())
    );
    
    if (directProductMatch) {
      matchedProduct = directProductMatch;
      console.log(`✅ Direct product match: "${matchedProduct}"`);
    }
    
    // Only use Gemini if we don't have direct matches
    if (!matchedCustomer || !matchedProduct) {
      try {
        const matchText = await callGeminiWithRetry(matchPrompt, { maxRetries: 1 });
        console.log(`🤖 Gemini match response: ${matchText}`);
        
        // Parse JSON response
        const cleanJson = matchText.replace(/```json\n?|\n?```/g, '').trim();
        const matchData = JSON.parse(cleanJson);
        
        if (!matchedCustomer) {
          matchedCustomer = matchData.matchedCustomer;
          customerConfidence = matchData.customerConfidence;
        }
        if (!matchedProduct) {
          matchedProduct = matchData.matchedProduct;
        }
        
        console.log(`✅ Final customer: "${matchedCustomer}" (${customerConfidence})`);
        console.log(`✅ Final product: "${matchedProduct}"`);
      } catch (matchError) {
        console.log('⚠️ Gemini matching failed, using direct matches only:', matchError.message);
        // Use customer as-is if Gemini failed but we have direct match attempt
        if (!matchedCustomer) matchedCustomer = customer;
        if (!matchedProduct) matchedProduct = product;
      }
    }
    
    // If customer not recognized, ask for clarification
    if (!matchedCustomer || customerConfidence === 'none' || customerConfidence === 'low') {
      return res.status(400).json({ 
        error: `Customer "${customer}" is not recognized. Should I add them as a new wholesale client?`,
        clarification: true,
        originalDetails: details,
        originalCustomer: customer,
        customers: getKnownCustomers()
      });
    }
    
    if (!matchedProduct) {
      return res.status(400).json({ error: `Could not find a product matching "${product}". Available: ${uniqueProducts.join(', ')}` });
    }
    
    // Use matched customer name
    let finalCustomer = matchedCustomer;
    console.log(`✅ Using customer: "${finalCustomer}"`);
    
    // Use Gemini to find the correct price from the sheet
    const pricingPrompt = `You are a pricing lookup assistant. Given the spreadsheet data below, find the correct per-pound price.

SPREADSHEET DATA (columns A through H):
${pricingRows.map((row, i) => `Row ${i + 1}: ${row.map((cell, j) => `${String.fromCharCode(65 + j)}="${cell || ''}"`).join(', ')}`).join('\n')}

PRICING RULES:
1. IMPORTANT: If customer is "Archives of Us" (or AOU), you MUST use the "At-Cost" table (around row 2) and get the price from column H ("Per lb"). For example, "Archives Blend" at-cost per lb is in cell H4.
2. For customer "CED", use "Wholesale CED" table and get price from column D
3. For customer "Dex", use "Wholesale Dex" table and get price from column D  
4. For customer "Junia", use "Wholesale Junia" table and get price from column D
5. For any new/unknown customer, use "Wholesale Tier 1" pricing from column D

LOOKUP REQUEST:
- Customer: "${finalCustomer}"
- Product: "${matchedProduct}"

The price should be a number like 10.22, 11.50, 14.00, etc. NOT zero.

Respond ONLY with valid JSON (no markdown, no explanation):
{
  "price": <number - this should NOT be zero or null>,
  "table": "<name of table used>",
  "row": <row number where product was found>,
  "column": "<H for At-Cost, D for Wholesale>",
  "explanation": "<brief explanation of how you found it>"
}`;

    let unitPrice = null;
    let pricingSource = null;
    
    // Try Gemini pricing lookup with retry
    try {
      const pricingText = await callGeminiWithRetry(pricingPrompt, { maxRetries: 2 });
      console.log(`🤖 Gemini pricing response: ${pricingText}`);
      
      const cleanJson = pricingText.replace(/```json\n?|\n?```/g, '').trim();
      const pricingData = JSON.parse(cleanJson);
      
      if (pricingData.price !== null && pricingData.price > 0) {
        unitPrice = parseFloat(pricingData.price);
        pricingSource = pricingData.table;
        console.log(`✅ Found price: $${unitPrice}/lb from ${pricingSource}`);
      }
    } catch (pricingError) {
      console.log('⚠️ Gemini pricing failed, using direct lookup:', pricingError.message);
    }
    
    // Fallback: Direct sheet parsing if Gemini failed
    if (!unitPrice || unitPrice <= 0) {
      console.log(`📋 Falling back to direct sheet lookup...`);
      
      // Determine which table and column to use
      const isArchives = finalCustomer.toLowerCase() === 'archives of us';
      const priceColumn = isArchives ? 7 : 3; // H (index 7) for At-Cost, D (index 3) for Wholesale
      
      // Find the correct table
      let targetTable = isArchives ? 'At-Cost' : `Wholesale ${finalCustomer}`;
      let tableStartRow = -1;
      
      for (let i = 0; i < pricingRows.length; i++) {
        const cellB = (pricingRows[i][1] || '').toString().toLowerCase();
        if (isArchives && cellB === 'at-cost') {
          tableStartRow = i;
          break;
        } else if (!isArchives && cellB.includes('wholesale') && cellB.includes(finalCustomer.toLowerCase())) {
          tableStartRow = i;
          break;
        }
      }
      
      // Fallback to Tier 1 if no specific table found
      if (tableStartRow === -1 && !isArchives) {
        for (let i = 0; i < pricingRows.length; i++) {
          const cellB = (pricingRows[i][1] || '').toString().toLowerCase();
          if (cellB.includes('wholesale tier 1')) {
            tableStartRow = i;
            targetTable = 'Wholesale Tier 1';
            break;
          }
        }
      }
      
      // Search for product in the table
      if (tableStartRow !== -1) {
        for (let i = tableStartRow + 1; i < pricingRows.length; i++) {
          const row = pricingRows[i];
          const cellB = (row[1] || '').toString().trim().toLowerCase();
          const priceCell = row[priceColumn];
          
          // Stop if we hit another table or empty row
          if (!cellB || cellB.includes('wholesale') || cellB === 'at-cost') break;
          if (cellB === 'coffee') continue; // Skip header
          
          if (cellB === matchedProduct.toLowerCase()) {
            unitPrice = parseFloat((priceCell || '').toString().replace(/[$,]/g, ''));
            pricingSource = targetTable + ' (direct lookup)';
            console.log(`✅ Direct lookup found: $${unitPrice}/lb from ${targetTable}`);
            break;
          }
        }
      }
    }
    
    if (!unitPrice || unitPrice <= 0) {
      return res.status(400).json({ 
        error: `Could not find valid pricing for "${matchedProduct}" for customer "${finalCustomer}". Please check the pricing sheet.`,
        debug: { customer: finalCustomer, product: matchedProduct, pricingSource }
      });
    }
    
    // Update product name to the matched version for the invoice
    const finalProduct = matchedProduct;

    console.log(`💰 Unit price for ${finalProduct}: $${unitPrice}/lb (from ${pricingSource})`);

    // Step 2: Get last invoice number from Invoices sheet using Gemini
    const invoicesResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!A:E'
    });
    
    const invoiceRows = invoicesResponse.data.values || [];
    const customerPrefix = getCustomerCode(finalCustomer);
    
    // Use Gemini to find the last invoice number for this customer
    const invoicePrompt = `You are an invoice number lookup assistant. Given the spreadsheet data below, find the highest invoice number for customer prefix "${customerPrefix}".

SPREADSHEET DATA (Invoices sheet):
${invoiceRows.map((row, i) => `Row ${i + 1}: ${row.map((cell, j) => `${String.fromCharCode(65 + j)}="${cell || ''}"`).join(', ')}`).join('\n')}

RULES:
1. Invoice numbers are in column C and follow the format: C-XXX-#### (e.g., C-CED-1000, C-AOU-1001)
2. Find all invoices that match the prefix "C-${customerPrefix}-"
3. Return the highest number found, or 999 if no invoices exist for this customer (so next one will be 1000)

Respond ONLY with valid JSON (no markdown, no explanation):
{
  "lastNumber": <highest invoice number found or 999 if none>,
  "invoicesFound": <count of invoices found for this customer>,
  "explanation": "<brief explanation>"
}`;

    let lastNumber = 999;
    
    // Try Gemini first with retry
    try {
      const invoiceText = await callGeminiWithRetry(invoicePrompt, { maxRetries: 1 });
      console.log(`🤖 Gemini invoice lookup: ${invoiceText}`);
      
      const cleanJson = invoiceText.replace(/```json\n?|\n?```/g, '').trim();
      const invoiceData = JSON.parse(cleanJson);
      
      lastNumber = invoiceData.lastNumber;
      console.log(`✅ Last invoice for ${customerPrefix}: ${lastNumber}`);
    } catch (invoiceError) {
      console.log('⚠️ Gemini invoice lookup failed, using direct search:', invoiceError.message);
      // Direct search in column C
      for (const row of invoiceRows) {
        if (row[2] && row[2].startsWith(`C-${customerPrefix}-`)) {
          const num = parseInt(row[2].split('-')[2]);
          if (!isNaN(num) && num > lastNumber) {
            lastNumber = num;
          }
        }
      }
      console.log(`✅ Direct search found last number: ${lastNumber}`);
    }
    
    const invoiceNumber = `C-${customerPrefix}-${lastNumber + 1}`;
    console.log(`🧾 Generated invoice number: ${invoiceNumber}`);

    // Step 3: Calculate totals
    const total = quantity * unitPrice;
    const today = new Date();
    const dateStr = today.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });
    const dueDateObj = new Date(today);
    dueDateObj.setDate(dueDateObj.getDate() + 2); // Due in 2 days like your example
    const dueDateStr = dueDateObj.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });

    // Step 4: Generate PDF
    const pdfFilename = `${invoiceNumber}.pdf`;
    const pdfPath = path.join(invoicesDir, pdfFilename);
    
    await generateInvoicePDF({
      invoiceNumber,
      customer: finalCustomer,
      date: dateStr,
      dueDate: dueDateStr,
      items: [{
        description: `${finalProduct} (units in lbs)`,
        quantity,
        unitPrice,
        total
      }],
      subtotal: total,
      total
    }, pdfPath);

    console.log(`📄 PDF generated: ${pdfPath}`);

    // Step 5: Record in Invoices sheet
    // Format: Column B = Date, Column C = Invoice #, Column D = Price, Column E = (blank for Paid)
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!B:E',
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: {
        values: [[dateStr, invoiceNumber, `$${total.toFixed(2)}`, '']]
      }
    });

    console.log(`✅ Invoice recorded in spreadsheet`);

    // Return response
    res.json({
      success: true,
      invoiceNumber,
      customer: finalCustomer,
      date: dateStr,
      dueDate: dueDateStr,
      quantity,
      product: finalProduct,
      unitPrice,
      total,
      pdfUrl: `/invoices/${pdfFilename}`
    });

  } catch (error) {
    console.error('Invoice generation error:', error);
    res.status(500).json({ error: 'Failed to generate invoice', details: error.message });
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
    
    // Quick pattern matching for common phrases (fallback when rate limited)
    const textLower = text.toLowerCase().trim();
    
    // Handle simple yes/no without AI
    if (textLower === 'no' || textLower === 'cancel' || textLower === 'never mind' || textLower === 'nope') {
      return res.json({
        response: "No problem!",
        action: 'declined',
        showFollowUp: true
      });
    }
    
    if ((textLower === 'yes' || textLower === 'yeah' || textLower === 'yep' || textLower.includes('yes, add')) && 
        conversationState === 'waiting_for_new_customer_confirmation') {
      return res.json({
        response: "Great! Adding them now...",
        action: 'confirm_add_customer',
        showFollowUp: false
      });
    }
    
    if (textLower === 'thanks' || textLower === 'thank you' || textLower === "that's all") {
      return res.json({
        response: "You're welcome!",
        action: 'completed',
        showFollowUp: true
      });
    }

    // Try to parse invoice pattern without AI first (for rate limit fallback)
    const invoicePattern = /(?:invoice|order|bill)?\s*(?:for\s+)?(\w+(?:'s)?(?:\s+\w+)?)\s*[,.]?\s*(\d+)\s*(?:lbs?|pounds?|lb)?\s*(?:of\s+)?(.+)/i;
    const simplePattern = /^(\w+(?:'s)?(?:\s+\w+)?)\s+(\d+)\s*(?:lbs?|pounds?|lb)?\s*(.+)$/i;
    
    let fallbackData = null;
    const match = text.match(invoicePattern) || text.match(simplePattern);
    if (match) {
      const customerInput = match[1].trim();
      const quantity = parseInt(match[2]);
      const product = match[3].trim();
      
      // Match customer
      const matchedCustomer = getKnownCustomers().find(c => 
        c.toLowerCase() === customerInput.toLowerCase() ||
        customerInput.toLowerCase().includes(c.toLowerCase()) ||
        c.toLowerCase().includes(customerInput.toLowerCase())
      );
      
      fallbackData = {
        intent: 'create_invoice',
        customer: matchedCustomer || customerInput,
        quantity,
        product,
        isKnownCustomer: !!matchedCustomer
      };
    }
    
    // Try Gemini with retry
    let intentData = null;
    try {
      const intentPrompt = `${SYSTEM_PROMPT}

KNOWN CUSTOMERS: ${getKnownCustomers().join(', ')}

CUSTOMER MATCHING RULES:
- "Dex", "Dex's Coffee", "deks", "dex coffee" → matches "Dex" (isKnownCustomer: true)
- "CED", "ced coffee" → matches "CED" (isKnownCustomer: true)
- "Archives of Us", "AOU", "archives", "aou coffee" → matches "Archives of Us" (isKnownCustomer: true)
- "Junia", "junia coffee", "juna" → matches "Junia" (isKnownCustomer: true)

CONVERSATION CONTEXT: ${conversationState || 'none'}

User said: "${text}"

Respond ONLY with valid JSON:
{
  "intent": "create_invoice" | "update_inventory" | "check_inventory" | "decline" | "confirm" | "general_question",
  "customer": "<matched customer name or original if new>",
  "quantity": <number or null>,
  "unit": "<lbs, bags, etc. or null>",
  "product": "<product name or null>",
  "isKnownCustomer": <true/false>,
  "friendlyResponse": "<brief response>",
  "conversationComplete": <true/false>
}`;

      const intentText = await callGeminiWithRetry(intentPrompt, { temperature: 0.1, maxRetries: 2 });
      console.log(`🤖 Intent detection: ${intentText}`);
      
      const cleanJson = intentText.replace(/```json\n?|\n?```/g, '').trim();
      intentData = JSON.parse(cleanJson);
      
    } catch (error) {
      if (error.message === 'RATE_LIMITED') {
        console.log('⚠️ Rate limited, using fallback pattern matching');
        if (fallbackData) {
          intentData = fallbackData;
        } else {
          return res.json({
            response: "I'm a bit busy right now. Please try again in a moment, or use the quick action buttons below.",
            action: 'rate_limited',
            showFollowUp: true
          });
        }
      } else {
        throw error;
      }
    }
    
    if (!intentData) {
      return res.json({ 
        response: "I didn't quite catch that. Could you rephrase?",
        action: null,
        showFollowUp: false
      });
    }
    
    // Handle decline/cancel
    if (intentData.intent === 'decline') {
      return res.json({
        response: intentData.friendlyResponse || "No problem!",
        action: 'declined',
        showFollowUp: true
      });
    }
    
    // Handle confirm (when user types "yes" to add new customer)
    if (intentData.intent === 'confirm' && conversationState === 'waiting_for_new_customer_confirmation') {
      return res.json({
        response: "Great! Adding them now...",
        action: 'confirm_add_customer',
        showFollowUp: false
      });
    }
    
    // Handle different intents
    if (intentData.intent === 'create_invoice') {
      if (intentData.customer && intentData.quantity && intentData.product) {
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
        
        if (!isActuallyKnown) {
          // Unknown customer - ask to add
          return res.json({
            response: `I don't recognize "${intentData.customer}" as a current wholesale client. Would you like me to add them as a new customer?`,
            action: 'confirm_new_customer',
            pendingInvoice: {
              customer: intentData.customer,
              quantity: intentData.quantity,
              unit: intentData.unit || 'lbs',
              product: intentData.product
            },
            showFollowUp: false
          });
        } else {
          // Known customer - proceed with invoice
          return res.json({
            response: `Got it! Creating an invoice for ${customerToUse} - ${intentData.quantity} ${intentData.unit || 'lbs'} of ${intentData.product}. Processing now...`,
            action: 'create_invoice',
            invoiceDetails: `${customerToUse} ${intentData.quantity} ${intentData.unit || 'lbs'} ${intentData.product}`,
            showFollowUp: false  // Follow-up will be added after invoice is generated
          });
        }
      } else {
        // Missing info for invoice
        return res.json({
          response: intentData.friendlyResponse || "I'd be happy to create an invoice! Could you provide the customer name, quantity, and product?",
          action: 'need_more_info',
          showFollowUp: false
        });
      }
    }
    
    // For other intents, return the friendly response with conversation state
    res.json({ 
      response: intentData.friendlyResponse || "How can I help you today?",
      action: intentData.intent,
      data: intentData,
      showFollowUp: intentData.conversationComplete === true
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