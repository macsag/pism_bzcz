import csv
from tqdm import tqdm
from pymarc import *
from permissive import PermissiveMARCReader
from collections import namedtuple

TsvEntry = namedtuple('TsvEntry', ['id', 'issn', 'title', 'issue'])
ParsedIssue = namedtuple('ParsedIssue',
                         ['date_from_brackets', 'all_numbers', 'numbers_before_equals', 'numbers_after_equals'])
SelectedRcd = namedtuple('SelectedRcd', ['rcd_id', 'rcd_773', 'rcd'])
LoadedDict = namedtuple('LoadedDict', ['issn_key_all_issues_in_dict',
                                       'issn_key_issues_in_list',
                                       'issn_key_range_issues_in_dict',
                                       'issn_key_issues_parsed_in_dict',
                                       'issn_key_range_issues_parsed_in_dict'])

def load_from_tsv(input_file):
    """
    Creates list of TsvEntry namedtuples from tsv file.
    TsvEntry attributes: ['id', 'issn', 'title', 'issue'].

    :param input_file:
    :return: list of TsvEntry objects (namedtuples)
    """
    loaded_data = []

    with open(input_file, 'r', newline='') as fp:
        r = csv.reader(fp, delimiter='\t')
        for line in tqdm(r):
            logging.debug(line)
            loaded_data.append(TsvEntry(line[0], line[1][3:], line[2][3:], line[3]))
    return loaded_data

def create_dictionaries(data):
    issn_key_all_issues_in_dict = {}

    issn_key_issues_in_list = {}
    issn_key_range_issues_in_dict = {}


    for entry in data:

        if entry.issn: # create entries only for issues with issn
            issn_key_all_issues_in_dict.setdefault(entry.issn, {}).setdefault(entry.issue, {'entry_data' : entry, 'entry_records': []})

            is_range = is_issue_range(entry.issue)
            logging.debug('{} - {}'.format(entry, is_range))

            if is_range and 'indeks' not in entry.issue:
                unstruct_range_list_1 = get_range_and_leave_double_issues(is_range[0][0])
                span_list = [is_range[0][1]]
                struct_range_dict = prepare_range_with_original_issue_structure(entry.issue, span_list, unstruct_range_list_1)

                if len(is_range) > 1:
                    unstruct_range_list_2 = get_range_and_leave_double_issues(is_range[1][0])
                    zipped_unstruct_list = zip(unstruct_range_list_1, unstruct_range_list_2)
                    span_list = (is_range[0][1], is_range[1][1])
                    struct_range_dict = prepare_range_with_original_issue_structure(entry.issue, span_list, zipped_unstruct_list)

                if len(struct_range_dict) > 2:
                    issn_key_range_issues_in_dict.setdefault(entry.issn, {}).update(struct_range_dict)
                    logging.debug('{}: {} - {}'.format('Added to secondary/range parsed issues list', entry.issn,
                                                       issn_key_range_issues_in_dict[entry.issn]))
                else:
                    issn_key_issues_in_list.setdefault(entry.issn, []).append(entry.issue)
                    logging.debug('{}: {} - {}'.format('Added to main issue list, (issue is range less than 2)', entry.issn,
                                                       issn_key_issues_in_list[entry.issn]))
            else:
                issn_key_issues_in_list.setdefault(entry.issn, []).append(entry.issue)
                logging.debug('{}: {} - {}'.format('Added to main issue list', entry.issn,
                                                   issn_key_issues_in_list[entry.issn]))



    issn_key_range_issues_parsed_in_dict = {}
    for key, value in issn_key_range_issues_in_dict.items():
        new_value = {}
        for el_key, el_value in value.items():
            new_value.update({parse_issue(el_key): el_value})
        logging.info(new_value)
        issn_key_range_issues_parsed_in_dict[key] = new_value

    issn_key_issues_parsed_in_dict = {}
    for key, value in issn_key_issues_in_list.items():
        new_value = {}
        for el in value:
            new_value.update({parse_issue(el): el})
        logging.info(new_value)
        issn_key_issues_parsed_in_dict[key] = new_value


    return LoadedDict(issn_key_all_issues_in_dict,
                      issn_key_issues_in_list,
                      issn_key_range_issues_in_dict,
                      issn_key_issues_parsed_in_dict,
                      issn_key_range_issues_parsed_in_dict)

def is_issue_range(issue):
    match = re.finditer(r'(\d+/?\d*)-(\d+/?\d*)', issue)
    list_to_return = []
    for m in match:
        list_to_return.append(((m.group(1), m.group(2)), m.span()))
    logging.debug(list_to_return)
    if list_to_return:
        return list_to_return
    else:
        return None

def get_range_and_leave_double_issues(range_tuple):
    orig_range_from = range_tuple[0]
    orig_range_to = range_tuple[1]
    range_from = range_tuple[0] if '/' not in range_tuple[0] else range_tuple[0].split('/')[1]
    if not range_from[-1].isdigit():
        range_from = range_from[:-1]
    range_to = range_tuple[1] if '/' not in range_tuple[1] else range_tuple[1].split('/')[0]
    if not range_to[-1].isdigit():
        range_to = range_to[:-1]

    try:
        list_range = list(range(int(range_from), int(range_to) + 1))
    except ValueError as e:
        logging.debug(e)
        return None

    if len(list_range) > 2:
        new_list_range = list_range[1:-1]
        new_list_range.append(orig_range_to)
        new_list_range.insert(0, orig_range_from)
        return new_list_range
    else:
        return [orig_range_from, orig_range_to]

def prepare_range_with_original_issue_structure(issue, span_list, issue_range):
    structured_range_dict = {}

    if len(span_list) == 1:
        for element in issue_range:
            span = span_list[0]
            str_element = str(element)
            structured_issue = '{}{}'.format(issue[:span[0]], str_element) #drop everything after str_element
            structured_range_dict.update({structured_issue: issue})
    else:
        for element in issue_range:
            first_str_element = str(element[0])
            second_str_element = str(element[1])
            structured_issue = '{}{}{}{}'.format(
                issue[:span_list[0][0]], first_str_element,
                issue[span_list[0][1] - 1:span_list[1][0]], second_str_element) #drop everything after second str_element
            structured_range_dict.update({structured_issue: issue})

    return structured_range_dict

def parse_issue(issue):

    if check_for_brackets(issue):
        date_from_brackets = get_date_in_brackets(issue)
    else:
        date_from_brackets = []
    if check_for_equals(issue):
        before_equals, after_equals = issue.split(' = ')
        numbers_before_equals = get_numbers_from_issue(before_equals)
        numbers_after_equals = get_numbers_from_issue(after_equals)
        numbers_from_issue = get_numbers_from_issue(issue)
    else:
        numbers_from_issue = get_numbers_from_issue(issue)
        numbers_before_equals = []
        numbers_after_equals = []

    ParsedIssue(date_from_brackets, numbers_from_issue, numbers_before_equals, numbers_after_equals)
    if numbers_before_equals and date_from_brackets:
        return date_from_brackets + ''.join(numbers_before_equals)
    if numbers_before_equals and not date_from_brackets:
        return ''.join(numbers_before_equals)
    if not numbers_before_equals:
        return ''.join(numbers_from_issue)



def get_date_in_brackets(issue):
    match = re.search(r'\(\d{4}\)', issue)
    if match:
        return match.group()[1:-1]
    else:
        return None

def check_for_brackets(issue):
    return True if '(' in issue and ')' in issue else False

def check_for_equals(issue):
    return True if ' = ' in issue else False

def get_numbers_from_issue(issue):
    match = re.findall(r'(\d{1,4}/?\d*)', issue)
    return match


# functions for selecting records
def check_for_001(rcd_object):
    if rcd_object.get_fields('001'):
        return rcd_object.get_fields('001')[0].value()
    else:
        return False

def check_for_773(rcd_object):
    output = False

    if rcd_object.get_fields('773'):
        fields_773 = rcd_object.get_fields('773')
        if len(fields_773) == 1:
            if fields_773[0].get_subfields('x') and fields_773[0].get_subfields('g'):
                output = fields_773[0]

    return output

def get_issn_and_issue_from_marc_record(selected_marc_record):
    output = None

    issn = selected_marc_record.rcd_773.get_subfields('x')[0][:-1]
    logging.debug(issn)

    issue_and_pages = selected_marc_record.rcd_773.get_subfields('g')[0]
    logging.debug(issue_and_pages)

    try:
        issue, pages = issue_and_pages.split(', s.')
        output = (issn, issue)
        logging.debug(issue)
    except ValueError as e:
        logging.debug(issue_and_pages)
        logging.debug(e)
        return output

    return output

def check_in_dict(issn, issue, dict_to_check):
    output = False

    if issn in dict_to_check:
        if issue in dict_to_check[issn]:
            output = True

    return output

def select_from_marc_file_records_with_773(marc_file):
    with open(marc_file, 'rb') as fp:
        rdr = PermissiveMARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore')

        for rcd in tqdm(rdr):
            rcd_id = check_for_001(rcd)
            if rcd_id:
                rcd_773 = check_for_773(rcd)
                if rcd_773:
                    yield SelectedRcd(rcd_id, rcd_773, rcd)
                else:
                    continue
            else:
                continue

def write_to_file(marc_file_out, marc_record):
    with open(marc_file_out, 'ab') as fp:
        fp.write(marc_record.as_marc())

def main_processing_loop(bn_marc_file_in, loaded_dictionaries):
    # files to export
    full_match = 'dopasowanie_pelne.mrc'
    range_match = 'dopasowanie_zakres_pelne.mrc'
    parsed_match = 'dopasowanie_sparsowane.mrc'
    parsed_range_match = 'dopasowanie_zakres_sparsowane.mrc'

    # start main loop
    for selected_marc_rcd in select_from_marc_file_records_with_773(bn_marc_file_in):
        issn_and_issue = get_issn_and_issue_from_marc_record(selected_marc_rcd)

        if not issn_and_issue:
            continue

        issn, issue = issn_and_issue
        id = selected_marc_rcd.rcd_id

        # check in dictionaries
        if check_in_dict(issn, issue, loaded_dictionaries.issn_key_issues_in_list):
            info = 'Rekord nr {} dopasowano (pełna zgodność numerów): 773: {} - PISM: {}'.format(id,
                                                                                                 selected_marc_rcd.rcd_773.value(),
                                                                                                 str(loaded_dictionaries.issn_key_all_issues_in_dict[issn][issue]['entry_data']))
            logging.info(info)
            loaded_dictionaries.issn_key_all_issues_in_dict[issn][issue]['entry_records'].append(info)
            write_to_file(full_match, selected_marc_rcd.rcd)
            continue

        if check_in_dict(issn, issue, loaded_dictionaries.issn_key_range_issues_in_dict):
            info = 'Rekord nr {} dopasowano (zgodność na podstawie zakresu numerów): 773: {} - PISM: {}'.format(id,
                                                                                                                selected_marc_rcd.rcd_773.value(),
                                                                                                                str(loaded_dictionaries.issn_key_all_issues_in_dict[issn][loaded_dictionaries.issn_key_range_issues_in_dict[issn][issue]]['entry_data']))
            logging.info(info)
            loaded_dictionaries.issn_key_all_issues_in_dict[issn][loaded_dictionaries.issn_key_range_issues_in_dict[issn][issue]]['entry_records'].append(info)
            write_to_file(range_match, selected_marc_rcd.rcd)
            continue

        parsed_issue = parse_issue(issue)
        if check_in_dict(issn, parsed_issue, loaded_dictionaries.issn_key_issues_parsed_in_dict):
            info = 'Rekord nr {} dopasowano (zgodność na podstawie sparsowanych numerów): 773: {} - PISM: {}'.format(id,
                                                                                                                     selected_marc_rcd.rcd_773.value(),
                                                                                                                     str(loaded_dictionaries.issn_key_all_issues_in_dict[issn][loaded_dictionaries.issn_key_issues_parsed_in_dict[issn][parsed_issue]]['entry_data']))
            logging.info(info)
            loaded_dictionaries.issn_key_all_issues_in_dict[issn][loaded_dictionaries.issn_key_issues_parsed_in_dict[issn][parsed_issue]]['entry_records'].append(info)
            write_to_file(parsed_match, selected_marc_rcd.rcd)
            continue

        if check_in_dict(issn, parsed_issue, loaded_dictionaries.issn_key_range_issues_parsed_in_dict):
            info = 'Rekord nr {} dopasowano (zgodność na podstawie sparsowanego zakresu numerów): 773: {} - PISM: {}'.format(id,
                                                                                                                             selected_marc_rcd.rcd_773.value(),
                                                                                                                             str(loaded_dictionaries.issn_key_all_issues_in_dict[issn][loaded_dictionaries.issn_key_range_issues_parsed_in_dict[issn][parsed_issue]]['entry_data']))
            logging.info(info)
            loaded_dictionaries.issn_key_all_issues_in_dict[issn][loaded_dictionaries.issn_key_range_issues_parsed_in_dict[issn][parsed_issue]]['entry_records'].append(info)
            write_to_file(parsed_range_match, selected_marc_rcd.rcd)

def create_log(loaded_dictionaries, log_file_out):
    with open(log_file_out, 'a', encoding='utf-8') as fp:
        for issn, issn_value in loaded_dictionaries.issn_key_all_issues_in_dict.items():
            fp.write('\n' + issn + '\n\n')
            for issue, issue_value in issn_value.items():
                fp.write('\n' + issue + '\n\n')
                for rcd in issue_value['entry_records']:
                    fp.write(rcd + '\n')


if __name__ == '__main__':
    logging.root.addHandler(logging.StreamHandler(sys.stdout))
    #logging.root.addHandler(logging.FileHandler('log_tylko_z_issn.txt', encoding='utf-8'))
    logging.root.setLevel(level=logging.INFO)

    tsv = 'pism2.tsv'
    bn_file = 'bibs-artykul.marc'
    log_file = 'log_nowy.txt'

    #main_processing_loop(tsv, bn_file, file_out)
    loaded_from_tsv = load_from_tsv(tsv)
    loaded_dict = create_dictionaries(loaded_from_tsv)

    main_processing_loop(bn_file, loaded_dict)
    create_log(loaded_dict, log_file)

