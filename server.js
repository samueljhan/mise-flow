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
console.log('AWS Access Key:', !!process.env.AWS_ACCESS_KEY_ID ? '✓' : '✗');
console.log('AWS Secret Key:', !!process.env.AWS_SECRET_ACCESS_KEY ? '✓' : '✗');
console.log('AWS Region:', process.env.AWS_REGION || 'us-east-1');
console.log('========================');

// System prompt for inventory assistant
const INVENTORY_SYSTEM_PROMPT = `You are Mise Flow, an AI assistant for small business inventory management. Your name comes from "mise en place" - the culinary practice of having everything in its place.

You help users:
1. Track inventory items (add, remove, update quantities)
2. Log deliveries and shipments
3. Set low-stock alerts
4. Generate inventory reports
5. Send emails to suppliers
6. Update spreadsheets

When parsing voice commands, extract:
- ACTION: add, remove, update, check, report, email, etc.
- ITEM: the inventory item name
- QUANTITY: number and unit (e.g., "5 bags", "10 lbs", "3 cases")
- NOTES: any additional context

Respond in a friendly, efficient manner. Be concise but helpful.

Example interactions:
- "Add 10 bags of espresso beans" → Extract: add, espresso beans, 10 bags
- "We're low on oat milk, order 5 cases" → Extract: alert + order, oat milk, 5 cases
- "Check stock on cups" → Extract: check, cups
- "Email the supplier about tomorrow's delivery" → Extract: email, supplier, delivery inquiry

Always confirm actions before executing them when they involve sending emails or modifying spreadsheets.`;

// ============ Google OAuth Routes ============

app.get('/auth/google', (req, res) => {
  const scopes = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.readonly',
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
    const { to, subject, body } = req.body;
    
    if (!to || !subject || !body) {
      return res.status(400).json({ error: 'Missing required fields: to, subject, body' });
    }
    
    oauth2Client.setCredentials(userTokens);
    const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
    
    const emailContent = [
      `To: ${to}`,
      `Subject: ${subject}`,
      'Content-Type: text/plain; charset=utf-8',
      '',
      body
    ].join('\n');
    
    const encodedEmail = Buffer.from(emailContent)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    
    await gmail.users.messages.send({
      userId: 'me',
      requestBody: { raw: encodedEmail }
    });
    
    console.log(`✅ Email sent to ${to}`);
    res.json({ success: true, message: `Email sent to ${to}` });
    
  } catch (error) {
    console.error('Email send error:', error);
    res.status(500).json({ error: 'Failed to send email', details: error.message });
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

// ============ Invoice Generation ============

app.post('/api/invoice/generate', async (req, res) => {
  if (!userTokens) {
    return res.status(401).json({ error: 'Google not connected. Please connect Google first.' });
  }

  try {
    const { details } = req.body;
    
    if (!details) {
      return res.status(400).json({ error: 'Invoice details required' });
    }

    // Parse the details (e.g., "CED, 100 lbs Archives Blend")
    const parsed = parseInvoiceDetails(details);
    if (!parsed) {
      return res.status(400).json({ error: 'Could not parse invoice details. Please use format: "Customer, Quantity lbs Product"' });
    }

    const { customer, quantity, product } = parsed;
    console.log(`📝 Generating invoice for: ${customer}, ${quantity} lbs ${product}`);

    oauth2Client.setCredentials(userTokens);
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });

    // Step 1: Get pricing from Wholesale Pricing sheet (entire sheet)
    const pricingResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Wholesale Pricing!A:D'
    });
    
    const pricingRows = pricingResponse.data.values || [];
    let unitPrice = null;
    
    // Structure:
    // - Table headers like "Wholesale CED" are in column B
    // - Row below header has "Coffee" (skip this)
    // - Products are in column B below that
    // - Prices are in column D
    
    let targetTable = null;
    let tableStartRow = -1;
    
    console.log(`🔍 Looking for customer: "${customer}", product: "${product}"`);
    
    // First pass: look for customer-specific table header in column B
    for (let i = 0; i < pricingRows.length; i++) {
      const row = pricingRows[i];
      const cellB = (row[1] || '').toString().toLowerCase();
      
      // Check if column B has "Wholesale [Customer]"
      if (cellB.includes('wholesale') && cellB.includes(customer.toLowerCase())) {
        targetTable = row[1];
        tableStartRow = i;
        console.log(`✅ Found table "${row[1]}" at row ${i + 1}`);
        break;
      }
    }
    
    // If no customer table found, look for "Wholesale Tier 1" as fallback
    if (tableStartRow === -1) {
      console.log(`⚠️ No table for "${customer}", trying Wholesale Tier 1...`);
      for (let i = 0; i < pricingRows.length; i++) {
        const row = pricingRows[i];
        const cellB = (row[1] || '').toString().toLowerCase();
        
        if (cellB.includes('wholesale tier 1')) {
          targetTable = 'Wholesale Tier 1';
          tableStartRow = i;
          console.log(`✅ Using fallback Wholesale Tier 1 at row ${i + 1}`);
          break;
        }
      }
    }
    
    // Search for product starting after the header row
    // Skip the "Coffee" header row (first row after table header)
    if (tableStartRow !== -1) {
      for (let i = tableStartRow + 2; i < pricingRows.length; i++) { // +2 to skip table header AND "Coffee" row
        const row = pricingRows[i];
        const cellB = (row[1] || '').toString().toLowerCase().trim();
        const cellD = row[3]; // Per lb price in column D
        
        // Stop if we hit another table header in column B
        if (cellB.includes('wholesale')) {
          break;
        }
        
        // Stop if we hit an empty row (end of table)
        if (!cellB) {
          break;
        }
        
        // Check if column B contains the product
        if (cellB.includes(product.toLowerCase())) {
          console.log(`✅ Found "${product}" at row ${i + 1}, price: ${cellD}`);
          if (cellD) {
            unitPrice = parseFloat(cellD.toString().replace(/[$,]/g, ''));
          }
          break;
        }
      }
    }
    
    // If still not found, do a full sheet search for the product in column B
    if (!unitPrice) {
      console.log(`⚠️ Product not found in table, searching entire sheet...`);
      for (const row of pricingRows) {
        const cellB = (row[1] || '').toString().toLowerCase().trim();
        const cellD = row[3];
        
        if (cellB.includes(product.toLowerCase()) && cellD) {
          unitPrice = parseFloat(cellD.toString().replace(/[$,]/g, ''));
          console.log(`✅ Found in fallback search, price: ${cellD}`);
          break;
        }
      }
    }
    
    if (!unitPrice) {
      return res.status(400).json({ error: `Product "${product}" not found in Wholesale Pricing sheet` });
    }
    
    console.log(`📋 Using pricing from: ${targetTable || 'default'}`)

    console.log(`💰 Unit price for ${product}: $${unitPrice}/lb`);

    // Step 2: Get last invoice number from Invoices sheet
    const invoicesResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!A:D'
    });
    
    const invoiceRows = invoicesResponse.data.values || [];
    const customerPrefix = customer.substring(0, 3).toUpperCase();
    let lastNumber = 999; // Start at 999 so first invoice is 1000
    
    for (const row of invoiceRows) {
      if (row[1] && row[1].startsWith(`C-${customerPrefix}-`)) {
        const num = parseInt(row[1].split('-')[2]);
        if (num > lastNumber) {
          lastNumber = num;
        }
      }
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
      customer,
      date: dateStr,
      dueDate: dueDateStr,
      items: [{
        description: `${product} (units in lbs)`,
        quantity,
        unitPrice,
        total
      }],
      subtotal: total,
      total
    }, pdfPath);

    console.log(`📄 PDF generated: ${pdfPath}`);

    // Step 5: Record in Invoices sheet
    await sheets.spreadsheets.values.append({
      spreadsheetId: SPREADSHEET_ID,
      range: 'Invoices!A:D',
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: {
        values: [[dateStr, invoiceNumber, customer, `$${total.toFixed(2)}`]]
      }
    });

    console.log(`✅ Invoice recorded in spreadsheet`);

    // Return response
    res.json({
      success: true,
      invoiceNumber,
      customer,
      date: dateStr,
      dueDate: dueDateStr,
      quantity,
      product,
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
    const { text, context } = req.body;
    
    if (!text) {
      return res.status(400).json({ error: 'Text is required' });
    }

    const model = genAI.getGenerativeModel({ 
      model: "gemini-2.5-flash",
      generationConfig: {
        temperature: 0.3,
        maxOutputTokens: 1000,
      }
    });
    
    const prompt = `${INVENTORY_SYSTEM_PROMPT}\n\nContext: ${context || 'General inventory management'}\n\nUser said: "${text}"\n\nParse this and respond with:\n1. What action to take\n2. A friendly confirmation message\n3. If applicable, structured data (JSON) for the action`;

    const result = await model.generateContent(prompt);
    const response = await result.response;

    res.json({ response: response.text() });
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