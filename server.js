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
    maxAge: 7 * 24 * 60 * 60 * 1000
  }
}));

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ============ AI Configuration ============

// OpenAI (ChatGPT) - for interpretation and conversational tasks
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

// Gemini - for Google Workspace operations
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// ============ AI Helper Functions ============

/**
 * Build context from Google Sheets for ChatGPT
 * This gives ChatGPT full visibility into the current state of the spreadsheet
 */
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
      
      // Fetch multiple sheets for full context
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

/**
 * Format sheet context as a string for ChatGPT prompts
 */
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
${Object.values(context.customers).map(c => `- ${c.name} (${c.code}): ${c.emails.join(', ') || 'no email'}`).join('\n')}
`;

  // Add raw sheet data if available (for more complex queries)
  if (context.sheetData?.invoices?.length > 0) {
    contextStr += `\nRECENT INVOICES (last 10):\n`;
    const recentInvoices = context.sheetData.invoices.slice(-10);
    recentInvoices.forEach(row => {
      if (row[1]) contextStr += `- ${row.join(' | ')}\n`;
    });
  }

  return contextStr;
}

/**
 * ChatGPT: Handle all interpretive and conversational tasks
 * - Understanding user intent
 * - Natural language parsing
 * - Conversational responses
 * - Complex decision making
 * 
 * @param systemPrompt - The system prompt defining ChatGPT's role
 * @param userMessage - The user's message to interpret
 * @param options.includeSheetContext - If true, fetches and includes current sheet data
 * @param options.sheetContext - Pre-fetched sheet context (to avoid multiple fetches)
 */
async function callChatGPT(systemPrompt, userMessage, options = {}) {
  const maxRetries = options.maxRetries || 3;
  
  // Build sheet context if requested
  let fullSystemPrompt = systemPrompt;
  if (options.includeSheetContext) {
    const context = options.sheetContext || await buildSheetContextForChatGPT();
    fullSystemPrompt += '\n\n' + formatSheetContextForPrompt(context);
  }
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await openai.chat.completions.create({
        model: options.model || 'gpt-4o-mini',
        messages: [
          { role: 'system', content: fullSystemPrompt },
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

/**
 * ChatGPT interprets data and generates Gemini instructions
 * This is the key handoff: ChatGPT decides WHAT to do, Gemini does HOW
 */
async function chatGPTToGeminiHandoff(userRequest, sheetContext) {
  const interpretationPrompt = `You are analyzing a user request and determining what Google Sheets/Gmail operations are needed.

Based on the current sheet state and user request, determine:
1. What data needs to be read from sheets?
2. What data needs to be written/updated?
3. What emails need to be drafted?

Respond with JSON:
{
  "interpretation": "what the user wants in plain English",
  "operations": [
    {
      "type": "sheets_read|sheets_write|sheets_update|gmail_draft",
      "target": "sheet name or email recipient",
      "details": "specific cells, values, or content",
      "reason": "why this operation is needed"
    }
  ],
  "response_to_user": "what to tell the user about what's happening"
}`;

  const response = await callChatGPT(
    interpretationPrompt,
    userRequest,
    { 
      jsonMode: true, 
      includeSheetContext: true,
      sheetContext: sheetContext 
    }
  );
  
  return JSON.parse(response);
}

/**
 * ChatGPT interprets results from Gemini/Google operations
 */
async function interpretGeminiResults(operation, results, originalRequest) {
  const interpretPrompt = `You are interpreting the results of a Google Sheets/Gmail operation for the user.

Original user request: "${originalRequest}"
Operation performed: ${operation}
Results: ${JSON.stringify(results)}

Provide a natural, conversational summary of what happened and what the data means.
Keep it concise but informative. If there are any issues or notable findings, highlight them.`;

  return await callChatGPT(interpretPrompt, 'Interpret these results', { temperature: 0.5 });
}

/**
 * Gemini: Handle all Google Workspace operations
 * - Reading/writing Google Sheets
 * - Gmail operations
 * - Any Google API interactions
 * 
 * When ChatGPT needs to manipulate Google data, it tells Gemini what to do
 */
async function callGeminiForGoogleOps(instructions, options = {}) {
  const maxRetries = options.maxRetries || 3;
  const model = genAI.getGenerativeModel({ 
    model: options.model || "gemini-2.5-flash",
    generationConfig: { temperature: options.temperature || 0 }
  });
  
  const prompt = `You are a Google Workspace operations assistant. Execute the following instructions precisely.
Your job is to translate high-level instructions into exact Google Sheets/Gmail operations.

INSTRUCTIONS:
${instructions}

Respond with JSON only:
{
  "action": "sheets_read|sheets_write|sheets_append|gmail_draft|gmail_send",
  "parameters": { ... specific parameters for the action ... },
  "description": "brief description of what you're doing"
}`;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await model.generateContent(prompt);
      return result.response.text().trim();
    } catch (error) {
      const isRateLimit = error.message?.includes('429') || error.message?.includes('quota');
      
      if (isRateLimit && attempt < maxRetries) {
        const retryMatch = error.message?.match(/retry in (\d+)/i);
        const waitTime = retryMatch ? parseInt(retryMatch[1]) * 1000 : (attempt * 2000);
        console.log(`⏳ Gemini rate limited, waiting ${waitTime/1000}s before retry ${attempt + 1}/${maxRetries}...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      } else if (isRateLimit) {
        throw new Error('GEMINI_RATE_LIMITED');
      } else {
        throw error;
      }
    }
  }
}

/**
 * Direct Gemini call with retry (for backward compatibility)
 */
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

// ============ System Prompts for ChatGPT ============

const MISE_SYSTEM_PROMPT = `You are Mise Flow, an AI assistant for Archives of Us Coffee. Your name comes from "mise en place" - the culinary practice of having everything in its place.

PERSONALITY:
- Warm, professional, and efficient
- Concise responses (1-3 sentences unless more detail needed)
- Action-oriented - identify what needs to be done and do it
- Never show raw JSON to users

YOU HELP WITH:
1. **Invoicing** - Create invoices for wholesale customers
2. **Inventory** - Track stock, log deliveries, set alerts
3. **Orders** - Document and process roast orders
4. **Communication** - Email suppliers and customers
5. **Reporting** - Generate reports and update spreadsheets

KNOWN WHOLESALE CUSTOMERS: Archives of Us, CED, Dex, Junia

SMART PATTERN RECOGNITION:
- "[Customer] [Quantity] [Product]" → Invoice request
- "check inventory" or "stock" → Inventory check
- "order roast" or coffee names → Roast order
- "email" or "send" → Email action

RESPONSE APPROACH:
1. Understand what the user wants
2. Identify the action needed
3. Respond conversationally while executing the action
4. Never ask unnecessary questions if intent is clear`;

const INVOICE_PARSER_PROMPT = `You are parsing invoice requests for Archives of Us Coffee.

KNOWN CUSTOMERS: Archives of Us (AOU), CED, Dex, Junia
PRODUCTS: Archives Blend, Ethiopia Gera, Colombia Excelso, Colombia Decaf

PRICING:
- Archives of Us uses "At-Cost" pricing
- All other customers use "Wholesale Tier 1" pricing

Parse the user's request and extract:
- Customer name (must match known customer or identify as new)
- Items with quantities (in pounds/lb)
- Any special notes

Respond with JSON:
{
  "understood": true/false,
  "customer": "customer name or null",
  "isNewCustomer": true/false,
  "items": [{"product": "name", "quantity": number, "unit": "lb"}],
  "notes": "any special notes",
  "clarificationNeeded": "question if unclear, null otherwise"
}`;

const ROAST_ORDER_PARSER_PROMPT = `You are parsing roast order requests for Archives of Us Coffee.

AVAILABLE ROASTED COFFEES:
- Archives Blend (Blend - Brazil/Ethiopia)
- Ethiopia Gera (Single Origin)
- Colombia Excelso (Single Origin)
- Colombia Decaf (Private Label)

NICKNAME RECOGNITION:
- "Blend" or "Archives" → Archives Blend
- "Ethiopia" or "Gera" → Ethiopia Gera
- "Colombia" or "Excelso" → Colombia Excelso
- "Decaf" → Colombia Decaf

Parse the request and extract coffee names and quantities.

Respond with JSON:
{
  "understood": true/false,
  "coffees": [{"name": "exact coffee name", "weight": number}],
  "needsClarification": true/false,
  "clarificationQuestion": "question if needed",
  "suggestedCoffee": "coffee name if suggesting"
}`;

const INTENT_CLASSIFIER_PROMPT = `You classify user intents for a coffee business management system.

POSSIBLE INTENTS:
- inventory: Check stock levels, view inventory
- roast_order: Order coffee to be roasted
- invoice: Generate invoice for customer
- en_route: View shipments in transit
- manage_inventory: Edit inventory levels
- email: Send or draft email
- payment: Record or track payments
- chat: General conversation or unclear

Respond with JSON:
{
  "intent": "one of the intents above",
  "confidence": "high/medium/low",
  "parameters": {},
  "response": "brief acknowledgment of what you understood"
}`;

// ============ Google OAuth Configuration ============

const oauth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.NODE_ENV === 'production' 
    ? process.env.GOOGLE_REDIRECT_URI || 'https://yourdomain.com/auth/google/callback'
    : 'http://localhost:8080/auth/google/callback'
);

let userTokens = null;

if (process.env.GOOGLE_REFRESH_TOKEN) {
  userTokens = { refresh_token: process.env.GOOGLE_REFRESH_TOKEN };
  oauth2Client.setCredentials(userTokens);
  console.log('📝 Google refresh token loaded from environment');
}

// AWS Transcribe configuration
const transcribeClient = new TranscribeStreamingClient({
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

// Archives of Us Coffee Spreadsheet ID
const SPREADSHEET_ID = '1D5JuAEpOC2ZXD2IAel1ImBXqFUrcMzFY-gXu4ocOMCw';

// Invoices directory
const invoicesDir = path.join(__dirname, 'public', 'invoices');
if (!fs.existsSync(invoicesDir)) {
  fs.mkdirSync(invoicesDir, { recursive: true });
}

console.log('=== Environment Check ===');
console.log('OpenAI:', !!process.env.OPENAI_API_KEY ? '✓' : '✗');
console.log('Gemini:', !!process.env.GEMINI_API_KEY ? '✓' : '✗');
console.log('Google Client ID:', !!process.env.GOOGLE_CLIENT_ID ? '✓' : '✗');
console.log('Google Client Secret:', !!process.env.GOOGLE_CLIENT_SECRET ? '✓' : '✗');
console.log('Google Refresh Token:', !!process.env.GOOGLE_REFRESH_TOKEN ? '✓ (auto-connect)' : '✗');
console.log('AWS Access Key:', !!process.env.AWS_ACCESS_KEY_ID ? '✓' : '✗');
console.log('========================');

// ============ Customer Directory ============

let customerDirectory = {
  'archives of us': { name: 'Archives of Us', code: 'AOU', emails: ['nick@archivesofus.com'] },
  'ced': { name: 'CED', code: 'CED', emails: ['songs0519@hotmail.com'] },
  'dex': { name: 'Dex', code: 'DEX', emails: ['dexcoffeeusa@gmail.com', 'lkusacorp@gmail.com'] },
  'junia': { name: 'Junia', code: 'JUN', emails: ['hello@juniacafe.com'] }
};

function getKnownCustomers() {
  return Object.values(customerDirectory).map(c => c.name);
}

function getCustomerCode(customerName) {
  const lower = customerName.toLowerCase();
  return customerDirectory[lower]?.code || customerName.substring(0, 3).toUpperCase();
}

function getCustomerEmails(customerName) {
  const lower = customerName.toLowerCase();
  return customerDirectory[lower]?.emails || [];
}

function addOrUpdateCustomer(name, code, emails = []) {
  customerDirectory[name.toLowerCase()] = { name, code: code.toUpperCase(), emails };
}

// ============ Coffee Inventory Data ============

let greenCoffeeInventory = [
  { id: 'colombia-antioquia', name: 'Colombia Antioquia', weight: 100, roastProfile: '122302', dropTemp: 410 },
  { id: 'ethiopia-gera', name: 'Ethiopia Gera', weight: 100, roastProfile: '061901', dropTemp: 414 },
  { id: 'brazil-mogiano', name: 'Brazil Mogiano', weight: 400, roastProfile: '199503', dropTemp: 419 },
  { id: 'ethiopia-yirgacheffe', name: 'Ethiopia Yirgacheffe', weight: 100, roastProfile: '141402', dropTemp: 415 }
];

let roastedCoffeeInventory = [
  { id: 'archives-blend', name: 'Archives Blend', weight: 150, type: 'Blend',
    recipe: [
      { greenCoffeeId: 'brazil-mogiano', name: 'Brazil Mogiano', percentage: 66.6667 },
      { greenCoffeeId: 'ethiopia-yirgacheffe', name: 'Ethiopia Yirgacheffe', percentage: 33.3333 }
    ]
  },
  { id: 'ethiopia-gera-roasted', name: 'Ethiopia Gera', weight: 40, type: 'Single Origin',
    recipe: [{ greenCoffeeId: 'ethiopia-gera', name: 'Ethiopia Gera', percentage: 100 }]
  },
  { id: 'colombia-excelso', name: 'Colombia Excelso', weight: 50, type: 'Single Origin',
    recipe: [{ greenCoffeeId: 'colombia-antioquia', name: 'Colombia Antioquia', percentage: 100 }]
  },
  { id: 'colombia-decaf', name: 'Colombia Decaf', weight: 30, type: 'Private Label', recipe: null }
];

let enRouteCoffeeInventory = [];

// ============ Authentication Middleware ============

function requireAuth(req, res, next) {
  if (req.session && req.session.authenticated) return next();
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ error: 'Authentication required', redirect: '/login' });
  }
  return res.redirect('/login');
}

// ============ Auth Routes ============

app.get('/login', (req, res) => {
  if (req.session && req.session.authenticated) return res.redirect('/');
  res.send(getLoginPage());
});

app.post('/auth/password', (req, res) => {
  const { username, password } = req.body;
  const validUser = process.env.APP_USERNAME || 'admin';
  const validPass = process.env.APP_PASSWORD;
  
  if (!validPass) {
    return res.redirect('/login?error=password_disabled');
  }
  
  if (username === validUser && password === validPass) {
    req.session.authenticated = true;
    req.session.user = { name: username, email: '', authMethod: 'password' };
    return res.redirect('/');
  }
  
  res.redirect('/login?error=invalid');
});

app.get('/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/login');
});

app.get('/auth/google', (req, res) => {
  const scopes = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.compose',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile'
  ];
  const url = oauth2Client.generateAuthUrl({ access_type: 'offline', scope: scopes, prompt: 'consent' });
  res.redirect(url);
});

app.get('/auth/google/callback', async (req, res) => {
  try {
    const { tokens } = await oauth2Client.getToken(req.query.code);
    oauth2Client.setCredentials(tokens);
    userTokens = tokens;

    const oauth2 = google.oauth2({ version: 'v2', auth: oauth2Client });
    const userInfo = await oauth2.userinfo.get();
    
    req.session.authenticated = true;
    req.session.user = {
      email: userInfo.data.email,
      name: userInfo.data.name,
      picture: userInfo.data.picture
    };

    req.session.save((err) => {
      if (err) return res.redirect('/login?error=session_failed');
      res.redirect('/');
    });
  } catch (error) {
    console.error('Auth callback error:', error);
    res.redirect('/login?error=auth_failed');
  }
});

// ============ API Routes ============

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    service: 'Mise Flow API',
    interpretation: 'ChatGPT (OpenAI)',
    googleOps: 'Gemini',
    transcription: 'AWS Transcribe',
    google: userTokens ? 'connected' : 'not connected'
  });
});

app.get('/api/user', requireAuth, (req, res) => {
  res.json({ authenticated: true, user: req.session.user });
});

// Google connection status - required by frontend
app.get('/api/google/status', requireAuth, (req, res) => {
  res.json({ 
    connected: !!userTokens,
    hasRefreshToken: !!process.env.GOOGLE_REFRESH_TOKEN
  });
});

// ============ Main Chat Processing - ChatGPT with Sheet Visibility ============

/**
 * Process user input using ChatGPT WITH full sheet context
 * ChatGPT sees the current state of inventory, invoices, etc.
 * Then delegates Google operations to Gemini
 */
app.post('/api/chat/process', async (req, res) => {
  await ensureFreshInventory();
  
  const { userInput, currentState } = req.body;
  
  // Build full sheet context for ChatGPT
  const sheetContext = await buildSheetContextForChatGPT();

  try {
    // Use ChatGPT to interpret with FULL sheet visibility
    const response = await callChatGPT(
      INTENT_CLASSIFIER_PROMPT + `\n\nCurrent state: ${currentState || 'idle'}`,
      userInput,
      { 
        jsonMode: true,
        includeSheetContext: true,
        sheetContext: sheetContext
      }
    );
    
    const parsed = JSON.parse(response);
    res.json({
      action: parsed.intent,
      parameters: parsed.parameters,
      chatResponse: parsed.response,
      confidence: parsed.confidence
    });
  } catch (error) {
    console.error('Chat process error:', error);
    res.json({ action: 'chat', chatResponse: "I'm having trouble understanding. Could you try rephrasing?" });
  }
});

/**
 * Advanced query endpoint - ChatGPT analyzes sheets and responds intelligently
 * Use this for complex questions like "what's my best selling coffee?" or "which invoices are unpaid?"
 */
app.post('/api/chat/query', async (req, res) => {
  await ensureFreshInventory();
  
  const { query } = req.body;
  
  if (!query) {
    return res.status(400).json({ error: 'Query required' });
  }
  
  // Build full sheet context
  const sheetContext = await buildSheetContextForChatGPT();
  
  const analysisPrompt = `You are Mise, an intelligent assistant for Archives of Us Coffee.
You have full visibility into the business's Google Sheets data including inventory and invoices.

Answer the user's question based on the actual data. Be specific with numbers and details.
If you need to perform calculations (like totals, averages, trends), do so.
If the data doesn't contain enough information to answer, say so clearly.

Keep your response conversational but informative.`;

  try {
    const response = await callChatGPT(
      analysisPrompt,
      query,
      { 
        includeSheetContext: true,
        sheetContext: sheetContext,
        temperature: 0.4
      }
    );
    
    res.json({ response, sheetDataUsed: !!sheetContext.sheetData });
  } catch (error) {
    console.error('Query error:', error);
    res.json({ response: "I couldn't analyze that query. Please try again." });
  }
});

/**
 * Smart operation endpoint - ChatGPT decides what Gemini should do
 * For complex operations that need interpretation + execution
 */
app.post('/api/chat/smart-operation', async (req, res) => {
  await ensureFreshInventory();
  
  const { request } = req.body;
  
  if (!request) {
    return res.status(400).json({ error: 'Request required' });
  }
  
  try {
    // Step 1: ChatGPT interprets the request with full sheet context
    const sheetContext = await buildSheetContextForChatGPT();
    const interpretation = await chatGPTToGeminiHandoff(request, sheetContext);
    
    console.log('ChatGPT interpretation:', interpretation);
    
    // Step 2: Execute the operations via Gemini/Google APIs
    const results = [];
    for (const op of interpretation.operations) {
      let result = { operation: op.type, status: 'pending' };
      
      try {
        switch (op.type) {
          case 'sheets_read':
            // Read from sheets
            if (userTokens) {
              oauth2Client.setCredentials(userTokens);
              const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
              const data = await sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: op.details
              });
              result = { operation: op.type, status: 'success', data: data.data.values };
            }
            break;
            
          case 'sheets_write':
          case 'sheets_update':
            // Write to sheets
            if (userTokens) {
              oauth2Client.setCredentials(userTokens);
              const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
              await sheets.spreadsheets.values.update({
                spreadsheetId: SPREADSHEET_ID,
                range: op.target,
                valueInputOption: 'USER_ENTERED',
                requestBody: { values: op.details }
              });
              result = { operation: op.type, status: 'success' };
            }
            break;
            
          case 'gmail_draft':
            // Create email draft
            if (userTokens) {
              oauth2Client.setCredentials(userTokens);
              const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
              const emailContent = [
                `To: ${op.target}`,
                `Subject: ${op.details.subject}`,
                'Content-Type: text/plain; charset=utf-8',
                '',
                op.details.body
              ].join('\n');
              
              const encodedEmail = Buffer.from(emailContent)
                .toString('base64')
                .replace(/\+/g, '-')
                .replace(/\//g, '_')
                .replace(/=+$/, '');
              
              await gmail.users.drafts.create({
                userId: 'me',
                requestBody: { message: { raw: encodedEmail } }
              });
              result = { operation: op.type, status: 'success' };
            }
            break;
        }
      } catch (opError) {
        result = { operation: op.type, status: 'error', error: opError.message };
      }
      
      results.push(result);
    }
    
    // Step 3: ChatGPT interprets the results for the user
    const summary = await interpretGeminiResults(
      interpretation.interpretation,
      results,
      request
    );
    
    res.json({
      interpretation: interpretation.interpretation,
      operations: interpretation.operations,
      results,
      summary,
      userMessage: interpretation.response_to_user
    });
    
  } catch (error) {
    console.error('Smart operation error:', error);
    res.json({ error: "I couldn't process that request. Please try again." });
  }
});

/**
 * Generate conversational response using ChatGPT with context
 */
app.post('/api/chat/respond', async (req, res) => {
  const { context, completedAction, inventory } = req.body;
  
  // Use provided inventory or fetch fresh
  const sheetContext = inventory ? { inventory } : await buildSheetContextForChatGPT();

  try {
    const response = await callChatGPT(
      MISE_SYSTEM_PROMPT,
      `Generate a brief, natural follow-up message. ${context ? `Context: ${context}` : ''} ${completedAction ? `Just completed: ${completedAction}` : ''}
Be concise (1-2 sentences). No emojis. Ask what else you can help with.`,
      { 
        temperature: 0.7,
        includeSheetContext: true,
        sheetContext: sheetContext
      }
    );
    
    res.json({ message: response });
  } catch (error) {
    console.error('Chat respond error:', error);
    res.json({ message: 'What else can I help you with?' });
  }
});

// ============ Roast Order Processing - Sheet-Aware ChatGPT + Gemini ============

/**
 * Parse roast order using ChatGPT WITH sheet context
 * ChatGPT sees current inventory levels to make smart suggestions
 */
app.post('/api/roast-order/parse', async (req, res) => {
  await ensureFreshInventory();
  
  const { userInput, previousQuestion, previousSuggestion } = req.body;
  
  // Build context including previous conversation state
  let conversationContext = '';
  if (previousQuestion && previousSuggestion) {
    conversationContext = `
PREVIOUS CONVERSATION:
- Mise asked: "${previousQuestion}"
- Suggested coffee: "${previousSuggestion}"
- User now responded: "${userInput}"

If user confirms (yes, yeah, yep, etc.), they're confirming "${previousSuggestion}".
`;
  }

  try {
    // ChatGPT parses with full inventory visibility
    const response = await callChatGPT(
      ROAST_ORDER_PARSER_PROMPT + conversationContext,
      userInput,
      { 
        jsonMode: true,
        includeSheetContext: true  // ChatGPT sees current stock levels
      }
    );
    
    res.json(JSON.parse(response));
  } catch (error) {
    console.error('Parse roast order error:', error);
    res.json({ 
      understood: false, 
      needsClarification: true, 
      clarificationQuestion: "Which roasted coffees would you like to order? For example: 'Archives Blend and Ethiopia Gera'" 
    });
  }
});

/**
 * Modify roast order using ChatGPT with inventory awareness
 */
app.post('/api/roast-order/modify', async (req, res) => {
  await ensureFreshInventory();
  
  const { userRequest, currentOrder } = req.body;
  
  // Get sheet context so ChatGPT can validate against actual inventory
  const sheetContext = await buildSheetContextForChatGPT();
  
  const prompt = `Parse this roast order modification request.
You have full visibility into current inventory - use it to validate the order is possible.

CURRENT ORDER: ${JSON.stringify(currentOrder.map(o => ({ name: o.name, weight: o.weight })))}
USER REQUEST: "${userRequest}"

NICKNAME RECOGNITION:
- "Blend", "Archives" → Archives Blend
- "Ethiopia", "Gera" → Ethiopia Gera
- "Colombia", "Excelso" → Colombia Excelso
- "Decaf" → Colombia Decaf

Check if the requested amounts are feasible based on green coffee inventory.
If not enough green coffee, warn the user but still process the order.

Respond with JSON:
{
  "success": true/false,
  "orderItems": [{"name": "exact coffee name", "weight": number}],
  "message": "brief acknowledgment",
  "warnings": ["any inventory warnings"] 
}`;

  try {
    const response = await callChatGPT(
      prompt, 
      userRequest, 
      { 
        jsonMode: true,
        includeSheetContext: true,
        sheetContext: sheetContext
      }
    );
    const parsed = JSON.parse(response);
    
    if (!parsed.success) {
      return res.json({ success: false, message: parsed.message || "I couldn't understand that modification." });
    }
    
    // Validate and build order items
    const orderItems = [];
    for (const item of parsed.orderItems) {
      const roastedCoffee = roastedCoffeeInventory.find(c => 
        c.name.toLowerCase() === item.name.toLowerCase()
      );
      
      if (roastedCoffee) {
        orderItems.push({
          name: roastedCoffee.name,
          weight: item.weight,
          type: roastedCoffee.type,
          recipe: roastedCoffee.recipe
        });
      }
    }
    
    if (orderItems.length === 0) {
      return res.json({ 
        success: false, 
        message: "I couldn't match those coffees to our inventory. Available: " + roastedCoffeeInventory.map(c => c.name).join(', ') 
      });
    }
    
    // Build summary HTML with any warnings
    let summaryHtml = '';
    if (parsed.warnings && parsed.warnings.length > 0) {
      summaryHtml += `<div style="color:#ffa500; margin-bottom:12px;">⚠️ ${parsed.warnings.join('<br>⚠️ ')}</div>`;
    }
    summaryHtml += '<div style="margin-bottom: 12px;"><strong>Updated Roast Order:</strong></div>';
    
    const getNickname = (name) => {
      const nicknames = {
        'Brazil Mogiano': 'Brazil', 'Colombia Decaf': 'Decaf', 'Colombia Antioquia': 'Colombia',
        'Ethiopia Yirgacheffe': 'Yirgacheffe', 'Ethiopia Gera': 'Ethiopia'
      };
      return nicknames[name] || name;
    };
    
    const calcBatches = (totalWeight) => {
      if (totalWeight <= 65) return { batches: 1, batchWeight: Math.round(totalWeight) };
      const batches = Math.ceil(totalWeight / 65);
      return { batches, batchWeight: Math.round(totalWeight / batches) };
    };
    
    for (const item of orderItems) {
      const roastedCoffee = roastedCoffeeInventory.find(c => c.name === item.name);
      
      if (roastedCoffee?.type === 'Blend' && roastedCoffee.recipe) {
        summaryHtml += `<strong>${item.name}</strong> (~${Math.round(item.weight)}lb roasted):<br>`;
        for (const comp of roastedCoffee.recipe) {
          const green = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
          const compGreenWeight = Math.round((item.weight / 0.85) * comp.percentage / 100);
          const { batches, batchWeight } = calcBatches(compGreenWeight);
          const nickname = getNickname(comp.name);
          
          // Show warning if low on green coffee
          const availableGreen = green?.weight || 0;
          const lowStock = availableGreen < compGreenWeight;
          
          summaryHtml += `- ${batches} batch${batches > 1 ? 'es' : ''} of ${nickname} (${batchWeight}lb - profile ${green?.roastProfile || '?'} - drop temp ${green?.dropTemp || '?'})`;
          if (lowStock) {
            summaryHtml += ` <span style="color:#ffa500;">[Only ${availableGreen}lb available!]</span>`;
          }
          summaryHtml += '<br>';
        }
        summaryHtml += '<br>';
      } else if (roastedCoffee?.type === 'Single Origin' && roastedCoffee.recipe) {
        const comp = roastedCoffee.recipe[0];
        const green = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
        const greenWeight = Math.round(item.weight / 0.85);
        const { batches, batchWeight } = calcBatches(greenWeight);
        const nickname = getNickname(roastedCoffee.name);
        
        const availableGreen = green?.weight || 0;
        const lowStock = availableGreen < greenWeight;
        
        summaryHtml += `- ${batches} batch${batches > 1 ? 'es' : ''} of ${nickname} (${batchWeight}lb - profile ${green?.roastProfile || '?'} - drop temp ${green?.dropTemp || '?'})`;
        if (lowStock) {
          summaryHtml += ` <span style="color:#ffa500;">[Only ${availableGreen}lb available!]</span>`;
        }
        summaryHtml += '<br>';
        summaryHtml += `<em style="color:#888; margin-left:12px;">~${Math.round(item.weight)}lb roasted</em><br><br>`;
      } else {
        const nickname = getNickname(roastedCoffee?.name || item.name);
        summaryHtml += `- ${Math.round(item.weight)}lb private label ${nickname}<br><br>`;
      }
    }
    
    summaryHtml += '<div style="color:#888; font-size:12px; margin-bottom:12px;">*Using max 65lb batches minimizes roasting costs</div>';
    summaryHtml += '<div class="response-buttons" style="margin-top: 12px;">';
    summaryHtml += '<button class="action-btn" onclick="confirmDefaultOrder()">Confirm</button>';
    summaryHtml += '<button class="action-btn" onclick="openEditOrderModal()">Edit Order</button>';
    summaryHtml += '<button class="action-btn" onclick="cancelRoastOrder()">Cancel</button>';
    summaryHtml += '</div>';
    
    res.json({ success: true, orderItems, summary: summaryHtml, warnings: parsed.warnings });
    
  } catch (error) {
    console.error('Modify roast order error:', error);
    res.json({ success: false, message: "Error processing modification. Please try again." });
  }
});

// ============ Invoice Processing - ChatGPT Interpretation ============

app.post('/api/invoice/parse', async (req, res) => {
  const { userInput } = req.body;
  
  const contextInfo = `
Known customers: ${getKnownCustomers().join(', ')}
Available products: ${roastedCoffeeInventory.map(c => c.name).join(', ')}
`;

  try {
    const response = await callChatGPT(
      INVOICE_PARSER_PROMPT + '\n' + contextInfo,
      userInput,
      { jsonMode: true }
    );
    
    res.json(JSON.parse(response));
  } catch (error) {
    console.error('Invoice parse error:', error);
    res.json({ 
      understood: false, 
      clarificationNeeded: "I couldn't parse that invoice request. Please specify customer and items." 
    });
  }
});

// Generate invoice PDF and data
app.post('/api/invoice/generate', requireAuth, async (req, res) => {
  const { customer, items, pricing } = req.body;
  
  if (!customer || !items || items.length === 0) {
    return res.status(400).json({ error: 'Customer and items required' });
  }
  
  try {
    const customerCode = getCustomerCode(customer);
    const invoiceNumber = `C-${customerCode}-${Date.now().toString().slice(-4)}`;
    const date = new Date().toLocaleDateString();
    
    // Determine pricing tier
    const isAtCost = customer.toLowerCase().includes('archives of us');
    const pricePerLb = isAtCost ? 6.50 : 8.00; // At-Cost vs Wholesale
    
    let total = 0;
    const lineItems = items.map(item => {
      const amount = item.quantity * pricePerLb;
      total += amount;
      return {
        product: item.product,
        quantity: item.quantity,
        unit: item.unit || 'lb',
        price: pricePerLb,
        amount
      };
    });
    
    res.json({
      success: true,
      invoice: {
        invoiceNumber,
        date,
        customer,
        customerCode,
        lineItems,
        subtotal: total,
        total,
        pricing: isAtCost ? 'At-Cost' : 'Wholesale'
      }
    });
  } catch (error) {
    console.error('Invoice generate error:', error);
    res.status(500).json({ error: 'Failed to generate invoice' });
  }
});

// Confirm invoice and record it
app.post('/api/invoice/confirm', requireAuth, async (req, res) => {
  const { invoice, sendEmail } = req.body;
  
  if (!invoice) {
    return res.status(400).json({ error: 'Invoice data required' });
  }
  
  try {
    // Record invoice to Google Sheets if connected
    if (userTokens) {
      oauth2Client.setCredentials(userTokens);
      const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
      
      await sheets.spreadsheets.values.append({
        spreadsheetId: SPREADSHEET_ID,
        range: 'Invoices!A:F',
        valueInputOption: 'USER_ENTERED',
        insertDataOption: 'INSERT_ROWS',
        requestBody: {
          values: [[
            invoice.date,
            invoice.invoiceNumber,
            invoice.customer,
            invoice.total,
            '', // Paid status - empty initially
            invoice.pricing
          ]]
        }
      }).catch(err => console.log('Could not record invoice to Sheets:', err.message));
    }
    
    // Deduct from roasted inventory
    for (const item of invoice.lineItems) {
      const roasted = roastedCoffeeInventory.find(c => 
        c.name.toLowerCase() === item.product.toLowerCase()
      );
      if (roasted) {
        roasted.weight -= item.quantity;
        if (roasted.weight < 0) roasted.weight = 0;
      }
    }
    
    await syncInventoryToSheets();
    
    res.json({
      success: true,
      invoiceNumber: invoice.invoiceNumber,
      message: `Invoice ${invoice.invoiceNumber} confirmed and recorded`
    });
  } catch (error) {
    console.error('Invoice confirm error:', error);
    res.status(500).json({ error: 'Failed to confirm invoice' });
  }
});

// ============ Interpret Yes/No - ChatGPT ============

app.post('/api/interpret-confirmation', async (req, res) => {
  const { message } = req.body;
  
  if (!message) {
    return res.status(400).json({ error: 'Message required' });
  }

  try {
    const response = await callChatGPT(
      `Interpret if the user is saying YES or NO to a question.
Respond with JSON only:
{"confirmed": true} if agreeing (yes, sure, ok, yeah, yep, go ahead, do it, sounds good)
{"confirmed": false} if declining (no, nope, cancel, never mind, don't)
{"confirmed": null} if unclear`,
      message,
      { jsonMode: true }
    );
    
    res.json(JSON.parse(response));
  } catch (error) {
    console.error('Error interpreting confirmation:', error);
    // Fallback
    const lower = message.toLowerCase();
    if (/yes|sure|ok|yep|yeah/.test(lower)) {
      res.json({ confirmed: true });
    } else if (/no|nope|cancel/.test(lower)) {
      res.json({ confirmed: false });
    } else {
      res.json({ confirmed: null });
    }
  }
});

// ============ Google Sheets Operations - Gemini Execution ============

/**
 * Sync inventory TO Google Sheets using Gemini
 */
async function syncInventoryToSheets() {
  if (!userTokens) {
    console.log('⚠️ Cannot sync inventory - Google not connected');
    return { success: false, error: 'Google not connected' };
  }

  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    const now = new Date();
    const pstTimestamp = now.toLocaleString('en-US', {
      timeZone: 'America/Los_Angeles',
      month: 'long', day: 'numeric', year: 'numeric',
      hour: 'numeric', minute: '2-digit', hour12: true
    }) + ' PST';

    // Build combined inventory data
    const data = [];
    data.push(['', '', '', '', '', '', '']);
    data.push(['', `Last updated: ${pstTimestamp}`, '', '', '', '', '']);
    data.push(['', '', '', '', '', '', '']);
    
    // Green coffee section
    data.push(['', 'Green Coffee Inventory', '', '', '', '', '']);
    data.push(['', 'Name', 'Weight (lb)', 'Roast Profile', 'Drop Temp', '', '']);
    greenCoffeeInventory.forEach(c => {
      data.push(['', c.name, c.weight, c.roastProfile || '', c.dropTemp || '', '', '']);
    });
    data.push(['', '', '', '', '', '', '']);
    
    // Roasted coffee section
    data.push(['', 'Roasted Coffee Inventory', '', '', '', '', '']);
    data.push(['', 'Name', 'Weight (lb)', 'Type', 'Recipe', '', '']);
    roastedCoffeeInventory.forEach(c => {
      const recipe = c.recipe ? c.recipe.map(r => `${r.percentage}% ${r.name}`).join(' + ') : 'N/A';
      data.push(['', c.name, c.weight, c.type || '', recipe, '', '']);
    });
    data.push(['', '', '', '', '', '', '']);
    
    // En route section
    data.push(['', 'En Route Inventory', '', '', '', '', '']);
    data.push(['', 'Name', 'Weight (lb)', 'Type', 'Tracking Number', 'Date Ordered', 'Estimated Ship Date']);
    if (enRouteCoffeeInventory.length > 0) {
      enRouteCoffeeInventory.forEach(c => {
        const estDelivery = c.trackingNumber ? (c.estimatedDelivery || '') : '';
        const dateOrdered = c.dateOrdered || c.orderDate || c.dateAdded || '';
        data.push(['', c.name, c.weight, c.type || '', c.trackingNumber || '', dateOrdered, estDelivery]);
      });
    }

    // Clear and write
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Inventory!A:G'
    }).catch(() => {});

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Inventory!A1',
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: data }
    }).catch(async () => {
      console.log('Creating Inventory sheet...');
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: 'Inventory' } } }] }
      });
      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: 'Inventory!A1',
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: data }
      });
    });

    console.log(`✅ Inventory synced to Google Sheets via Gemini`);
    return { success: true, timestamp: new Date().toISOString() };

  } catch (error) {
    console.error('❌ Inventory sync error:', error.message);
    return { success: false, error: error.message };
  }
}

/**
 * Load inventory FROM Google Sheets
 */
async function loadInventoryFromSheets() {
  if (!userTokens) {
    console.log('⚠️ Cannot load inventory - Google not connected');
    return { success: false, error: 'Google not connected' };
  }

  try {
    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Inventory!A:G'
    });
    
    const rows = response.data.values || [];
    if (rows.length === 0) {
      console.log('⚠️ Inventory sheet empty');
      return { success: false, error: 'Sheet empty' };
    }

    let currentSection = null;
    const tempGreen = [];
    const tempRoasted = [];
    const tempEnRoute = [];

    const parseRecipeString = (recipeStr) => {
      if (!recipeStr || recipeStr === 'N/A') return null;
      const recipe = [];
      const parts = recipeStr.split(/\s*\+\s*/);
      for (const part of parts) {
        const match = part.match(/^([\d.]+)%\s+(.+)$/);
        if (match) {
          recipe.push({
            greenCoffeeId: match[2].trim().toLowerCase().replace(/\s+/g, '-'),
            name: match[2].trim(),
            percentage: parseFloat(match[1])
          });
        }
      }
      return recipe.length > 0 ? recipe : null;
    };

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const cellB = (row[1] || '').toString().toLowerCase();
      
      if (cellB.includes('green coffee inventory')) { currentSection = 'green'; continue; }
      else if (cellB.includes('roasted coffee inventory')) { currentSection = 'roasted'; continue; }
      else if (cellB.includes('en route inventory')) { currentSection = 'enroute'; continue; }
      
      if (!row[1] || cellB === 'name' || cellB === '' || cellB.startsWith('last updated')) continue;
      
      if (currentSection === 'green' && row[1]) {
        tempGreen.push({
          id: row[1].toLowerCase().replace(/\s+/g, '-'),
          name: row[1],
          weight: parseFloat(row[2]) || 0,
          roastProfile: String(row[3] || '').replace(/\.0$/, ''),
          dropTemp: parseFloat(row[4]) || 0
        });
      } else if (currentSection === 'roasted' && row[1]) {
        const recipe = parseRecipeString(row[4] || '');
        let type = row[3] || '';
        if (!type && recipe) {
          type = recipe.length > 1 ? 'Blend' : 'Single Origin';
        } else if (!type) {
          type = 'Private Label';
        }
        tempRoasted.push({
          id: row[1].toLowerCase().replace(/\s+/g, '-'),
          name: row[1],
          weight: parseFloat(row[2]) || 0,
          type,
          recipe
        });
      } else if (currentSection === 'enroute' && row[1]) {
        tempEnRoute.push({
          id: `enroute-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          name: row[1],
          weight: parseFloat(row[2]) || 0,
          type: row[3] || '',
          trackingNumber: row[4] || '',
          dateOrdered: row[5] || '',
          estimatedDelivery: row[6] || ''
        });
      }
    }

    if (tempGreen.length > 0) greenCoffeeInventory = tempGreen;
    if (tempRoasted.length > 0) roastedCoffeeInventory = tempRoasted;
    enRouteCoffeeInventory = tempEnRoute;

    console.log(`✅ Loaded inventory from Sheets: ${greenCoffeeInventory.length} green, ${roastedCoffeeInventory.length} roasted, ${enRouteCoffeeInventory.length} en route`);
    return { success: true };

  } catch (error) {
    console.error('❌ Load inventory error:', error.message);
    return { success: false, error: error.message };
  }
}

let lastInventoryLoad = null;
const INVENTORY_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

async function ensureFreshInventory() {
  const now = Date.now();
  if (!lastInventoryLoad || (now - lastInventoryLoad) > INVENTORY_CACHE_TTL) {
    await loadInventoryFromSheets();
    lastInventoryLoad = now;
  }
}

// ============ Gmail Operations - Gemini Execution ============

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
      const boundary = 'boundary_' + Date.now();
      const pdfData = fs.readFileSync(attachmentPath);
      const pdfBase64 = pdfData.toString('base64');
      const filename = attachmentName || path.basename(attachmentPath);
      
      emailContent = [
        `To: ${to}`, `Subject: ${subject}`, 'MIME-Version: 1.0',
        `Content-Type: multipart/mixed; boundary="${boundary}"`, '',
        `--${boundary}`, 'Content-Type: text/plain; charset=utf-8', '', body, '',
        `--${boundary}`, `Content-Type: application/pdf; name="${filename}"`,
        'Content-Transfer-Encoding: base64',
        `Content-Disposition: attachment; filename="${filename}"`, '', pdfBase64, '',
        `--${boundary}--`
      ].join('\r\n');
    } else {
      emailContent = [`To: ${to}`, `Subject: ${subject}`, 'Content-Type: text/plain; charset=utf-8', '', body].join('\n');
    }
    
    const encodedEmail = Buffer.from(emailContent)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    
    const draft = await gmail.users.drafts.create({
      userId: 'me',
      requestBody: { message: { raw: encodedEmail } }
    });
    
    console.log(`📝 Draft created for ${to}`);
    res.json({ success: true, message: 'Draft created! Check your Gmail drafts folder.', draftId: draft.data.id });
    
  } catch (error) {
    console.error('Email draft error:', error);
    res.status(500).json({ error: 'Failed to create draft', details: error.message });
  }
});

// ============ Inventory API Endpoints ============

app.get('/api/inventory', requireAuth, async (req, res) => {
  await ensureFreshInventory();
  res.json({
    green: greenCoffeeInventory,
    roasted: roastedCoffeeInventory,
    enRoute: enRouteCoffeeInventory
  });
});

app.post('/api/inventory/sync', async (req, res) => {
  console.log('📤 Manual inventory sync triggered');
  const result = await syncInventoryToSheets();
  res.json(result);
});

app.post('/api/inventory/load', async (req, res) => {
  console.log('📥 Manual inventory load triggered');
  const result = await loadInventoryFromSheets();
  res.json(result);
});

// Update green coffee inventory
app.post('/api/inventory/green/update', requireAuth, async (req, res) => {
  const { id, weight } = req.body;
  
  const coffee = greenCoffeeInventory.find(c => c.id === id);
  if (coffee) {
    coffee.weight = parseFloat(weight) || 0;
    await syncInventoryToSheets();
    res.json({ success: true, coffee });
  } else {
    res.status(404).json({ error: 'Coffee not found' });
  }
});

// Update roasted coffee inventory
app.post('/api/inventory/roasted/update', requireAuth, async (req, res) => {
  const { id, weight } = req.body;
  
  const coffee = roastedCoffeeInventory.find(c => c.id === id);
  if (coffee) {
    coffee.weight = parseFloat(weight) || 0;
    await syncInventoryToSheets();
    res.json({ success: true, coffee });
  } else {
    res.status(404).json({ error: 'Coffee not found' });
  }
});

// Update en route tracking number
app.post('/api/inventory/enroute/tracking', requireAuth, async (req, res) => {
  const { id, trackingNumber } = req.body;
  
  const item = enRouteCoffeeInventory.find(c => c.id === id);
  if (item) {
    item.trackingNumber = trackingNumber || '';
    await syncInventoryToSheets();
    res.json({ success: true, item });
  } else {
    res.status(404).json({ error: 'En route item not found' });
  }
});

// Mark en route item as delivered (move to roasted inventory)
app.post('/api/inventory/enroute/deliver', requireAuth, async (req, res) => {
  const { id, actualWeight } = req.body;
  
  const index = enRouteCoffeeInventory.findIndex(c => c.id === id);
  if (index === -1) {
    return res.status(404).json({ error: 'En route item not found' });
  }
  
  const item = enRouteCoffeeInventory[index];
  const deliveredWeight = parseFloat(actualWeight) || item.weight;
  
  // Find or create roasted coffee entry
  const roasted = roastedCoffeeInventory.find(c => c.name === item.name);
  if (roasted) {
    roasted.weight += deliveredWeight;
  } else {
    roastedCoffeeInventory.push({
      id: item.name.toLowerCase().replace(/\s+/g, '-'),
      name: item.name,
      weight: deliveredWeight,
      type: item.type || 'Unknown',
      recipe: item.recipe || null
    });
  }
  
  // Remove from en route
  enRouteCoffeeInventory.splice(index, 1);
  
  await syncInventoryToSheets();
  
  res.json({ 
    success: true, 
    delivered: item.name, 
    weight: deliveredWeight,
    message: `${deliveredWeight}lb of ${item.name} added to roasted inventory`
  });
});

// UPS tracking lookup (placeholder - integrate with UPS API if needed)
app.post('/api/tracking/lookup', async (req, res) => {
  const { trackingNumber } = req.body;
  
  if (!trackingNumber) {
    return res.status(400).json({ error: 'Tracking number required' });
  }
  
  // For now, return a placeholder response
  // You can integrate with UPS/FedEx API here
  res.json({
    trackingNumber,
    status: 'In Transit',
    estimatedDelivery: 'Check carrier website for details',
    message: `Tracking ${trackingNumber} - please check UPS.com for live updates`
  });
});

// Legacy /api/process endpoint - redirect to new chat/process
app.post('/api/process', async (req, res) => {
  // Forward to the new chat process endpoint
  const { message } = req.body;
  
  await ensureFreshInventory();
  
  const sheetContext = await buildSheetContextForChatGPT();

  try {
    const response = await callChatGPT(
      INTENT_CLASSIFIER_PROMPT,
      message,
      { 
        jsonMode: true,
        includeSheetContext: true,
        sheetContext: sheetContext
      }
    );
    
    const parsed = JSON.parse(response);
    res.json({
      action: parsed.intent,
      parameters: parsed.parameters,
      response: parsed.response,
      confidence: parsed.confidence
    });
  } catch (error) {
    console.error('Process error:', error);
    res.json({ action: 'chat', response: "I'm having trouble understanding. Could you try rephrasing?" });
  }
});

// ============ Customers API ============

app.get('/api/customers', (req, res) => {
  res.json({ customers: getKnownCustomers() });
});

app.get('/api/customers/:name', (req, res) => {
  const lower = req.params.name.toLowerCase();
  if (customerDirectory[lower]) {
    res.json(customerDirectory[lower]);
  } else {
    res.status(404).json({ error: 'Customer not found' });
  }
});

app.post('/api/customers/add', (req, res) => {
  const { name, code, emails } = req.body;
  
  if (!name) return res.status(400).json({ error: 'Customer name required' });
  if (!code || code.length !== 3) return res.status(400).json({ error: 'Three letter code required' });
  
  const trimmedName = name.trim();
  const lower = trimmedName.toLowerCase();
  
  if (customerDirectory[lower]) {
    return res.status(400).json({ error: `Customer "${trimmedName}" already exists` });
  }
  
  addOrUpdateCustomer(trimmedName, code.trim(), emails || []);
  res.json({ success: true, message: `"${trimmedName}" added as new customer.`, customers: getKnownCustomers() });
});

app.post('/api/customers/emails', async (req, res) => {
  const { customerName, emailsInput } = req.body;
  
  if (!customerName || !emailsInput) {
    return res.status(400).json({ error: 'Customer name and emails required' });
  }
  
  // Use ChatGPT to parse emails from natural language
  try {
    const response = await callChatGPT(
      `Extract all email addresses from this input. Respond with JSON: {"emails": ["email1@example.com"]}`,
      emailsInput,
      { jsonMode: true }
    );
    
    const parsed = JSON.parse(response);
    
    if (!parsed.emails || parsed.emails.length === 0) {
      return res.status(400).json({ error: 'No valid email addresses found' });
    }
    
    const lower = customerName.toLowerCase();
    if (customerDirectory[lower]) {
      customerDirectory[lower].emails = parsed.emails;
      res.json({ success: true, emails: parsed.emails });
    } else {
      return res.status(404).json({ error: 'Customer not found' });
    }
  } catch (error) {
    console.error('Email parsing error:', error);
    // Fallback regex
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
    const emails = emailsInput.match(emailRegex) || [];
    
    if (emails.length === 0) {
      return res.status(400).json({ error: 'No valid email addresses found' });
    }
    
    const lower = customerName.toLowerCase();
    if (customerDirectory[lower]) {
      customerDirectory[lower].emails = emails;
      res.json({ success: true, emails });
    } else {
      return res.status(404).json({ error: 'Customer not found' });
    }
  }
});

// ============ Roast Order Confirmation ============

app.post('/api/roast-order/confirm', async (req, res) => {
  await ensureFreshInventory();
  
  const { orderItems, emailData } = req.body;
  
  const deductions = [];
  const enRouteItems = [];
  
  orderItems.forEach(item => {
    const roastedCoffee = roastedCoffeeInventory.find(c => c.name === item.name);
    
    if (roastedCoffee?.recipe) {
      roastedCoffee.recipe.forEach(comp => {
        const compRoastedWeight = item.weight * comp.percentage / 100;
        const greenWeight = Math.round(compRoastedWeight / 0.85);
        
        const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
        if (greenCoffee) {
          greenCoffee.weight -= greenWeight;
          deductions.push({ name: greenCoffee.name, deducted: greenWeight, remaining: greenCoffee.weight });
        }
      });
    }
    
    enRouteItems.push({
      id: `enroute-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      name: item.name,
      weight: item.weight,
      type: roastedCoffee?.type || 'Unknown',
      recipe: roastedCoffee?.recipe || null,
      trackingNumber: '',
      dateOrdered: new Date().toISOString()
    });
  });
  
  enRouteCoffeeInventory.push(...enRouteItems);
  
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
        requestBody: { message: { raw: encodedEmail } }
      });
      
      draftCreated = true;
      console.log('📝 Roast order draft created');
    } catch (error) {
      console.error('Failed to create draft:', error);
    }
  }
  
  await syncInventoryToSheets();
  
  res.json({
    success: true,
    deductions,
    enRouteItems,
    draftCreated,
    message: `Order confirmed! ${deductions.length > 0 ? 'Green coffee inventory updated.' : ''} ${enRouteItems.length} item(s) added to en route.${draftCreated ? ' Email draft created in Gmail.' : ''}`
  });
});

// ============ Generate Roast Order Email ============

app.post('/api/roast-order/generate-email', async (req, res) => {
  const { orderItems } = req.body;
  
  const today = new Date();
  const dateStr = `${today.getMonth() + 1}/${today.getDate()}/${today.getFullYear()}`;
  
  let emailBody = `Hi Shared Team,\n\nHope all is well! Would like to place a toll roast order for the following:\n\n`;
  
  const packagingItems = [];
  
  const getNickname = (name) => {
    const nicknames = {
      'Brazil Mogiano': 'Brazil', 'Colombia Decaf': 'Decaf', 'Colombia Antioquia': 'Colombia',
      'Ethiopia Yirgacheffe': 'Yirgacheffe', 'Ethiopia Gera': 'Ethiopia'
    };
    return nicknames[name] || name;
  };
  
  const calcBatches = (totalWeight) => {
    if (totalWeight <= 65) return { batches: 1, batchWeight: Math.round(totalWeight) };
    const batches = Math.ceil(totalWeight / 65);
    return { batches, batchWeight: Math.round(totalWeight / batches) };
  };
  
  orderItems.forEach(item => {
    const roastedCoffee = roastedCoffeeInventory.find(c => c.name === item.name);
    
    if (roastedCoffee?.type === 'Blend' && roastedCoffee.recipe) {
      const totalGreenWeight = Math.round(item.weight / 0.85);
      let blendParts = [];
      
      roastedCoffee.recipe.forEach(comp => {
        const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
        const compGreenWeight = Math.round(totalGreenWeight * comp.percentage / 100);
        const { batches, batchWeight } = calcBatches(compGreenWeight);
        
        if (greenCoffee) {
          blendParts.push(`${batches} batch${batches > 1 ? 'es' : ''} of ${getNickname(comp.name)} (${batchWeight}lb - profile ${greenCoffee.roastProfile} - drop temp ${greenCoffee.dropTemp})`);
        }
      });
      
      emailBody += `- ${blendParts.join(' blended with ')}\n`;
      const compNames = roastedCoffee.recipe.map(r => getNickname(r.name)).join('/');
      packagingItems.push(`~${Math.round(item.weight)}lb roasted ${compNames}`);
      
    } else if (roastedCoffee?.type === 'Single Origin' && roastedCoffee.recipe) {
      const comp = roastedCoffee.recipe[0];
      const greenCoffee = greenCoffeeInventory.find(g => g.id === comp.greenCoffeeId);
      const greenWeight = Math.round(item.weight / 0.85);
      const { batches, batchWeight } = calcBatches(greenWeight);
      
      if (greenCoffee) {
        emailBody += `- ${batches} batch${batches > 1 ? 'es' : ''} of ${getNickname(item.name)} (${batchWeight}lb - profile ${greenCoffee.roastProfile} - drop temp ${greenCoffee.dropTemp})\n`;
      }
      packagingItems.push(`~${Math.round(item.weight)}lb ${getNickname(item.name)}`);
      
    } else if (roastedCoffee?.type === 'Private Label') {
      emailBody += `- ${Math.round(item.weight)}lb private label ${getNickname(item.name)}\n`;
      packagingItems.push(`${Math.round(item.weight)}lb ${getNickname(item.name)}`);
    }
  });
  
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
    orderItems
  });
});

// ============ Static Files & Login Page ============

app.use((req, res, next) => {
  if (req.path === '/login' || req.path.startsWith('/auth/') || req.path === '/privacy' || req.path === '/api/health') {
    return next();
  }
  if (!req.session || !req.session.authenticated) {
    if (req.accepts('html') && !req.path.startsWith('/api/')) {
      return res.redirect('/login');
    }
    if (req.path.startsWith('/api/')) {
      return res.status(401).json({ error: 'Authentication required' });
    }
  }
  next();
});

app.use(express.static('public'));

function getLoginPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Mise Flow - Login</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a1a; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
    .login-container { background: #2a2a2a; border-radius: 12px; padding: 40px; max-width: 360px; width: 90%; }
    h1 { color: #e0e0e0; font-size: 1.6rem; margin-bottom: 6px; font-weight: 600; }
    .subtitle { color: #888; margin-bottom: 28px; font-size: 0.85rem; }
    .form-group { margin-bottom: 16px; }
    .form-group label { display: block; color: #aaa; font-size: 0.8rem; margin-bottom: 6px; }
    .form-group input { width: 100%; padding: 12px; background: #1a1a1a; border: 1px solid #3a3a3a; border-radius: 6px; color: #e0e0e0; font-size: 0.95rem; }
    .login-btn { width: 100%; padding: 12px; background: #3a3a3a; border: none; border-radius: 6px; color: #e0e0e0; font-size: 0.95rem; cursor: pointer; margin-bottom: 16px; }
    .login-btn:hover { background: #4a4a4a; }
    .divider { display: flex; align-items: center; margin: 20px 0; color: #555; font-size: 0.8rem; }
    .divider::before, .divider::after { content: ''; flex: 1; height: 1px; background: #3a3a3a; }
    .divider span { padding: 0 12px; }
    .google-btn { display: flex; align-items: center; justify-content: center; gap: 10px; width: 100%; padding: 12px; background: #fff; border: none; border-radius: 6px; color: #333; font-size: 0.95rem; cursor: pointer; text-decoration: none; }
    .google-btn:hover { background: #f0f0f0; }
    .error-msg { background: #442222; color: #ff6b6b; padding: 10px 12px; border-radius: 6px; margin-bottom: 20px; font-size: 0.85rem; display: none; }
  </style>
</head>
<body>
  <div class="login-container">
    <h1>Mise Flow</h1>
    <p class="subtitle">AI-powered Work Flow for AOU Coffee, Inc.</p>
    <div class="error-msg" id="errorMsg">Authentication failed. Please try again.</div>
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
      <svg viewBox="0 0 24 24" width="18" height="18">
        <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/>
        <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
        <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
        <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
      </svg>
      Sign in with Google
    </a>
  </div>
  <script>
    if (new URLSearchParams(window.location.search).get('error')) {
      document.getElementById('errorMsg').style.display = 'block';
    }
  </script>
</body>
</html>`;
}

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