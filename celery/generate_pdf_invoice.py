#! /usr/bin/env python
# 2013-06-24  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   used task for retrieving address_to
# 2013-04-01  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   nothing serious, just updated logging level
# 2013-02-08 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   changed address
# 2013-01-24 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add credit card form link, updated some text
# 2013-01-18 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   add paypal fee notes
# 2012-11-05 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   changed -18 days group to -30 days for old clients
# 2012-10-15  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   dont show Available balance for the -18 days group
# 2012-10-12  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   display invoice date
# 2012-10-11  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   display running balance 0 when clients running balance is negative
# 2012-10-09 Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   replaced earliest_starting_date with pay_before_date
# celery task for generating pdf
import settings
import string
import couchdb
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from celery.task import task
from celery.execute import send_task

from celery import Celery
import sc_celeryconfig


from pytz import timezone
from decimal import Decimal
from datetime import date, datetime, timedelta

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Frame, Table
from reportlab.pdfgen.canvas import Canvas
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.rl_config import defaultPageSize
from reportlab.lib.units import inch
from reportlab.lib import colors

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

from cStringIO import StringIO

PAGE_HEIGHT=defaultPageSize[1]; PAGE_WIDTH=defaultPageSize[0]
styles = getSampleStyleSheet()
styleN = styles['Normal']
styleH = styles['Heading1']
styleH2 = styles['Heading2']

pageinfo = "platypus example"
logo = "images/remote_staff_logo.png"


def get_ph_time(as_array=False):
    """returns a philippines datetime
    """
    utc = timezone('UTC')
    phtz = timezone('Asia/Manila')
    now = utc.localize(datetime.utcnow())
    now = now.astimezone(phtz)
    if as_array:
        return [now.year, now.month, now.day, now.hour, now.minute, now.second]
    else:
        return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


def myFirstPage(canvas, doc):
    canvas.saveState()
    canvas.drawImage(logo, 0.5*inch, PAGE_HEIGHT-80, 2.5*inch, 0.75*inch)
    canvas.setFont('Helvetica-Bold',11)

    canvas.setLineWidth(2)
    canvas.setStrokeColorRGB(0.22, 0.34, 0.53)
    canvas.setFillColorRGB(0.92, 0.96, 0.86)

    if doc.show_available_balance == True:
        canvas.roundRect(5.4*inch, PAGE_HEIGHT-68, 2.7*inch, 0.6*inch, 0.08*inch, stroke=1, fill=1)
    canvas.roundRect(5.4*inch, PAGE_HEIGHT-132, 2.7*inch, 0.8*inch, 0.08*inch, stroke=1, fill=1)

    canvas.setStrokeColorRGB(0, 0, 0)
    canvas.setFillColorRGB(0, 0, 0)

    #displays a running balance 0 when client has a negative running balance
    running_balance = doc.running_balance
    if running_balance <= Decimal('0.00'):
        running_balance = Decimal('0.00')

    if doc.show_available_balance == True:
        canvas.drawString(5.5*inch, PAGE_HEIGHT-52, "Available Balance: %s %s%s" % (doc.currency, doc.currency_sign, locale.format('%0.2f', running_balance, True)))
    if hasattr(doc, 'pay_before_date'):
        canvas.drawString(5.5*inch, PAGE_HEIGHT-94, "PAY NOW: %s %s%s" % (doc.currency, doc.currency_sign, doc.total_amount))
        canvas.setFont('Helvetica-Oblique',8)
        canvas.drawString(5.5*inch, PAGE_HEIGHT-108, "Invoice Date: %s" % (doc.invoice_date))
        canvas.drawString(5.5*inch, PAGE_HEIGHT-120, "Due Date: %s" % (doc.pay_before_date))
    else:
        canvas.drawString(5.5*inch, PAGE_HEIGHT-100, "PAY NOW: %s %s%s" % (doc.currency, doc.currency_sign, doc.total_amount))
        canvas.setFont('Helvetica-Oblique',8)
        canvas.drawString(5.5*inch, PAGE_HEIGHT-116, "Invoice Date: %s" % (doc.invoice_date))

    canvas.setLineWidth(0.5) #underline width

    flowables = []
    p = Paragraph("<b>Think Innovations Pty. Ltd T/A Remote Staff</b>", styleH2)
    flowables.append(p)
    styleN.backColor = None
    p = Paragraph("<i>ABN 37 094 364 511</i>", styleN)
    flowables.append(p)
    p = Paragraph("""<u><a color="blue" href="http://www.remotestaff.com.au">www.remotestaff.com.au</a></u> , <u><a color="blue" href="http://www.remotestaff.net">www.remotestaff.net</a></u><br/>&nbsp;<br/>
    104 / 529 Old South Head Road, Rose Bay, NSW 2029<br/>
    <b>Phone</b> : +61(02) 8014 9196<br/>
    <b>Fax</b> : 02 8088 7247<br/>
    <b>USA Fax</b> : (650) 745 1088<br/>
    <b>Email</b> : <u><a color="blue" href="mailto:accounts@remotestaff.com.au">accounts@remotestaff.com.au</a></u><br/>&nbsp<br/>
    """, styleN)
    flowables.append(p)
    
    #client_details

    
    if settings.DEBUG:
        task_client_name = send_task('EmailSender.GetAddressTo', [doc.client_id])
    else:
        celery = Celery()
        celery.config_from_object(sc_celeryconfig)
        task_client_name = celery.send_task('EmailSender.GetAddressTo', [doc.client_id])
    
    client_name = task_client_name.get()
    x = doc.client_details
    p = Paragraph("""Name: <b>%s</b><br/>
    Company: %s<br/>
    Address: %s<br/>&nbsp<br/>
    """ % (client_name, x.company_name, x.company_address), styleN)
    flowables.append(p)

    p = Paragraph("""<para alignment="center">TAX INVOICE %s</para>""" % doc.order_id, styleH)
    flowables.append(p)

    f = Frame(0.4*inch, PAGE_HEIGHT-288, 7.5*inch, 3.0*inch, showBoundary=0)
    f.addFromList(flowables, canvas)

    canvas.setFont('Helvetica',9)
    canvas.drawString(inch, 0.75 * inch, "Page 1")

##~    canvas.setLineWidth(1)
    canvas.restoreState()


def myLaterPages(canvas, doc):
    canvas.saveState()
    canvas.setFont('Helvetica',9)
    canvas.drawString(inch, 0.75 * inch, "Page %d" % (doc.page))
    canvas.restoreState()


@task
def generate(doc_id):
    """given the document id generate a pdf string output
    """
    s = couchdb.Server(settings.COUCH_DSN)
    db = s['client_docs']
    doc = db.get(doc_id)

    if doc == None:
        raise Exception('Failed to generate pdf.', 'Document %s not found.' % doc_id)

    client_id = doc['client_id']

    engine = create_engine(settings.MYSQL_DSN)
    conn = engine.connect()

    output_stream = StringIO()
    doc_pdf = SimpleDocTemplate(output_stream)

    #get fname, lname
    sql = text("""SELECT fname, lname, company_name, company_address from leads
        WHERE id = :client_id
        """)
    client_details = conn.execute(sql, client_id = client_id).fetchone()

    doc_pdf.client_id = client_id
    doc_pdf.client_details = client_details
    doc_pdf.order_id = doc['order_id']

    #check clients running balance
    r = db.view('client/running_balance', key=client_id)
    
    if len(r.rows) == 0:
        doc_pdf.running_balance = Decimal(0)
    else:
        doc_pdf.running_balance = Decimal('%0.2f' % r.rows[0].value)

    sql = text("""SELECT sign from currency_lookup
        WHERE code = :currency
        """)
    doc_pdf.currency_sign = conn.execute(sql, currency = doc['currency']).fetchone()['sign']
    doc_pdf.currency = doc['currency']
    doc_pdf.total_amount = locale.format('%0.2f', Decimal(doc['total_amount']), True)

    if doc.has_key('invoice_date'):
        x = doc['invoice_date']
    else:
        x = doc['added_on']
    doc_pdf.invoice_date = date(x[0], x[1], x[2]).strftime('%B %d, %Y')

    if doc.has_key('pay_before_date'):
        x = doc['pay_before_date']
        doc_pdf.pay_before_date = date(x[0], x[1], x[2]).strftime('%B %d, %Y')

    #get client settings     for the removal of Available Balance
    now = get_ph_time(as_array=True)
    view = db.view('client/settings', startkey=[client_id, now],
        endkey=[client_id, [2011,1,1,0,0,0,0]], 
        descending=True, limit=1, include_docs=True)
    doc_client_settings = view.rows[0].doc
    show_available_balance = True
    if doc_client_settings.has_key('days_before_suspension'):
        if doc_client_settings['days_before_suspension'] == -30:
            show_available_balance = False

    doc_pdf.show_available_balance = show_available_balance

    #spacer
    Story = [Spacer(1,3*inch)]

    #collect items
    items = doc['items']
    items_has_date = False

    styles = getSampleStyleSheet()
    style_table = styles['Normal']
    style_table.fontSize = 8
    p_num_hours = Paragraph('<b>Number of Hours / Quantity</b>', style_table)
    p_hourly_rate = Paragraph('<b>Hourly Rate / Unit Price %s</b>' % (doc_pdf.currency_sign), style_table) 
    pdf_items = [["Item", "Date", "Name and Designation of Staff", p_num_hours, p_hourly_rate, "Amount %s" % (doc_pdf.currency_sign)],]

    for item in items:
        item_date = ''
        if item.has_key('start_date'):
            items_has_date = True
            x = item['start_date']
            y = date(x[0], x[1], x[2])
            item_date += y.strftime('%b %d, %Y')

        if item.has_key('end_date'):
            items_has_date = True
            x = item['end_date']
            y = date(x[0], x[1], x[2])
            item_date += y.strftime(' - %b %d, %Y')


        description = Paragraph(item['description'], style_table)
        pdf_item = [
            item['item_id'],
            Paragraph(item_date, style_table),
            description,
            locale.format('%0.2f', Decimal(item['qty']), True),
            locale.format('%0.2f', Decimal(item['unit_price']), True),
            locale.format('%0.2f', Decimal(item['amount']), True)
        ]
        pdf_items.append(pdf_item)

    summary = []
    #append sub_total
    p_sub_total = Paragraph('<para alignment="right"><b>Sub Total</b></para>', style_table)
    summary.append([p_sub_total,'','','','', locale.format('%0.2f', Decimal(doc['sub_total']), True)])

    #append gst
    p_gst = Paragraph('<para alignment="right"><b>GST</b></para>', style_table)
    summary.append([p_gst,'','','','', locale.format('%0.2f', Decimal(doc['gst_amount']), True)])

    #append total_amount
    p_total_amount = Paragraph('<para alignment="right"><b>Total Amount</b></para>', style_table)
    summary.append([p_total_amount,'','','','', '%s %s%s' % (doc_pdf.currency, doc_pdf.currency_sign, locale.format('%0.2f', Decimal(doc['total_amount']), True))])

    grid_style = [
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('ALIGN', (0,0), (-1,0), 'CENTER'),
        ('ALIGN', (0,1), (0,-1), 'RIGHT'),
        ('ALIGN', (3,1), (3,-1), 'RIGHT'),
        ('ALIGN', (4,1), (4,-1), 'RIGHT'),
        ('ALIGN', (5,1), (5,-1), 'RIGHT'),
        ]

    width_date = 1*inch
    width_description = None
    if items_has_date == False:
        width_date = 0.4*inch
        width_description = 3.5*inch

    t = Table(pdf_items, 
        colWidths=[0.4*inch, width_date, width_description, 0.6*inch, 0.6*inch, 1*inch], 
        style=grid_style, repeatRows=1)
    Story.append(t)

    grid_style_summary = [
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('ALIGN', (5,0), (5,-1), 'RIGHT'),
        ('SPAN', (0,-1), (4,-1)),
        ('SPAN', (0,-2), (4,-2)),
        ('SPAN', (0,-3), (4,-3)),
        ('FONTNAME', (-1,-1), (-1,-1), 'Helvetica-Bold'),   #bold total_amount figure
        ('BACKGROUND',(-1,-1), (-1,-1),colors.yellow),  #change background color or total_amount figure
        ('GRID', (-1,-3), (-1,-1), 0.5, colors.grey),
        ]
    t_summary = Table(summary,
        colWidths=[0.4*inch, width_date, width_description, 0.6*inch, 0.6*inch, 1*inch], 
        style=grid_style_summary)
    Story.append(t_summary)

    p = Paragraph("""HOW TO PAY:""", styleH2)
    Story.append(p)
    Story.append(Spacer(1,0.1*inch))
    
    styles = getSampleStyleSheet()
    style = styles["Normal"]
    style.borderWidth = 1
    style.borderRadius = 0.08*inch
    style.borderPadding = 0.1*inch
    style.fontName = 'Helvetica'
    style.borderColor = (0.22, 0.34, 0.53)
    style.backColor = (0.92, 0.96, 0.86)
    style.spaceAfter = 0.25*inch

    #payment_link = 'https://remotestaff.com.au/portal/ClientTopUp/TopUp.html?order_id=%s' % doc['order_id']
    payment_link = 'https://remotestaff.com.au/portal/v2/payments/top-up/%s' % doc['order_id']
    p = Paragraph("""
    <b>PAY ONLINE OR PHONE USING YOUR CREDIT CARD</b><br/>&nbsp<br/>
    We accept Visa, Master Card and AMEX. Follow this link <a color="blue" href="%s">%s</a>
    to pay for this invoice or call +61(02) 8014 9196 press 4 to pay over the phone.
    """ % (payment_link, payment_link), style)
    Story.append(p)

    debit_form_link = 'https://remotestaff.com.au/portal/pdf_report/credit_card_debit_form/THKGENDirectDebitForm.pdf'
    credit_card_form_link = 'https://remotestaff.com.au/portal/pdf_report/credit_card_debit_form/?id=%s' % client_id 
    p = Paragraph("""
    <b>AUTO DEBIT</b><br/>&nbsp<br/>
    Have your account paid on the invoice due date automaticaly via direct debit or credit card to save time. Fill this form <a color="blue" href="%s">%s</a> (Australian Clients Only) or Credit Card form <a color="blue" href="%s">%s</a> and return to <a color="blue" href="mailto:accounts@remotestaff.com.au">accounts@remotestaff.com.au</a>
    """ % (debit_form_link, debit_form_link, credit_card_form_link, credit_card_form_link), style)
    Story.append(p)

    p = Paragraph("""
    <b>ELECTRONIC BANK TRANSFER TO</b><br/>&nbsp<br/>
    <b>Australia : </b><br/>
    Account Name: Think Innovations Pty. Ltd.<br/>
    BSB: 082 973<br/>
    Account Number: 49 058 9267<br/>
    Bank Branch: Darling Street, Balmain NSW 2041<br/>
    Swift Code: NATAAU3302S<br/>&nbsp<br/>
    <b>United Kingdom : </b><br/>
    Account Name: Think Innovations Pty. Ltd.<br/>
    UK Bank Address: HSBC. 25 Nothing Hill Gate. London. W11 3JJ<br/>
    Sort code: 40-05-09<br/>
    Acc: 61-50-63-23<br/>
    Swift Code: MIDLGB22<br/>
    IBAN Number: GB54MIDL40050961506323<br/>&nbsp;<br/>
    <b>United States : </b><br/>
    Account Name: Think Innovations Pty. Ltd.<br/>
    Bank Branch: HSBC Bank USA NA 452 Fifth Avenue, New York, NY 10018<br/>
    Account number: 048-984-515<br/>
    Routing Number: 021001088<br/>
    Swift code: MRMDUS33<br/>
    """, style)
    Story.append(p)

    p = Paragraph("""
    <b>Note:</b><br/>&nbsp;<br/>
    For Invoices in Australian Dollar a Merchant facility fees apply for the following credit card holders:
    <br/>
    """, styleN)
    Story.append(p)

    styleN.bulletIndent = 0.2*inch
    p = Paragraph("""<br/><bullet>AMEX : 2%</bullet>""", styleN)
    Story.append(p)

    p = Paragraph("""<br/><bullet>Visa / MasterCard : 1%</bullet>""", styleN)
    Story.append(p)

    p = Paragraph("""<br/>For Invoices in Pounds and USD, 2% Merchant facility fees apply for all credit card payments.""", styleN)
    Story.append(p)

    p = Paragraph("""<br/>Paypal fees ranging from 1.1% - 2.4% of your invoice amount applies and will be reflected as a debit on your Available Balance Sheet.""", styleN)
    Story.append(p)

    p = Paragraph("""<br/>Note that we prefer payments made via bank transfer or direct debit.""", styleN)
    Story.append(p)

    Story.append(Spacer(1,1*inch))

    styles_ref_doc = getSampleStyleSheet()
    style_ref_doc = styles_ref_doc['Normal']
    style_ref_doc.fontName = 'Courier'
    style_ref_doc.fontSize = 9
    p = Paragraph("""
    -----------------------------------------<br/>
    Ref doc: %s
    -----------------------------------------<br/>
    """ % doc_id, style_ref_doc)
    Story.append(p)

    doc_pdf.title = 'INVOICE %s' % doc['order_id']
    doc_pdf.subject = 'doc_id %s' % doc['_id']
    doc_pdf.author = 'remotestaff'
    doc_pdf.creator = 'celery task generate_pdf_invoice.generate'
    doc_pdf.build(Story, onFirstPage=myFirstPage, onLaterPages=myLaterPages)
    output_stream.seek(0)
    return output_stream.read()


if __name__ == '__main__':
    doc_str = generate('6104e0b0c146ee3ba1cb817cd47ee36e')
##~    doc_str = generate('38147e0db857b2f1122fb95e9d85d419')
##~    doc_str = generate('7f59280a7898f99c96255a7fe066a290')
##~    doc_str = generate('7cbf1e46e67d7292eeb2ac9cf315457f')
##~    doc_str = generate('f3aa7d291772b16573098f3249955d55')
##~    doc_str = generate('942d52ac9f6da09cda37dc43f3407edd')
##~    doc_str = generate('494ff507424d7d8ddf617d8d4104a930')
##~    doc_str = generate('ad3da614b2db939529a3948be073088a')
##~    doc_str = generate('a09040d00082f73237c93392f1b9c6db')
##~    doc_str = generate('379be80962d966cf373316e2a0d14c7a')
##~    doc_str = generate('8b53832e3fbeb55440dc675f195ca8ba')
##~    doc_str = generate('656308947e389e4735266b07a7ccbe0e')
    print doc_str
