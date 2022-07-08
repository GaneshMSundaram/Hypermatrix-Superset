/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React, {
  FunctionComponent,
  useState,
  ReactNode,
  useMemo,
  useEffect,
} from 'react';
import { styled, SupersetClient, t } from '@superset-ui/core';
import { Select } from 'src/components';
import { FormLabel } from 'src/components/Form';
import Icons from 'src/components/Icons';
import DatabaseSelector, {
  DatabaseObject,
} from 'src/components/DatabaseSelector';
import RefreshLabel from 'src/components/RefreshLabel';
import CertifiedBadge from 'src/components/CertifiedBadge';
import WarningIconWithTooltip from 'src/components/WarningIconWithTooltip';
import { useToasts } from 'src/components/MessageToasts/withToasts';
import {
  Accordion,
  AccordionItem,
  AccordionItemHeading,
  AccordionItemButton,
  AccordionItemPanel,
} from 'react-accessible-accordion';

// Demo styles, see 'Styles' section below for some notes on use.
import 'react-accessible-accordion/dist/fancy-example.css';
const tableItemArray: (TableOption[]) = [];

const TableSelectorWrapper = styled.div`
  ${({ theme }) => `
    .refresh {
      display: flex;
      align-items: center;
      width: 30px;
      margin-left: ${theme.gridUnit}px;
      margin-top: ${theme.gridUnit * 5}px;
    }

    .section {
      display: flex;
      flex-direction: row;
      align-items: center;
    }

    .divider {
      border-bottom: 1px solid ${theme.colors.secondary.light5};
      margin: 15px 0;
    }

    .table-length {
      color: ${theme.colors.grayscale.light1};
    }

    .select {
      flex: 1;
    }
  `}
`;

const TableLabel = styled.span`
  align-items: center;
  display: flex;
  white-space: nowrap;

  svg,
  small {
    margin-right: ${({ theme }) => theme.gridUnit}px;
  }
`;

interface TableSelectorProps {
  clearable?: boolean;
  database?: DatabaseObject;
  formMode?: boolean;
  getDbList?: (arg0: any) => {};
  handleError: (msg: string) => void;
  isDatabaseSelectEnabled?: boolean;
  onDbChange?: (db: DatabaseObject) => void;
  onSchemaChange?: (schema?: string) => void;
  onSchemasLoad?: () => void;
  onTableChange?: (tableName?: string, schema?: string) => void;
  onTablesLoad?: (options: Array<any>) => void;
  readOnly?: boolean;
  schema?: string;
  sqlLabMode?: boolean;
  tableName?: string;
}

interface Table {
  label: string;
  value: string;
  type: string;
  extra?: {
    certification?: {
      certified_by: string;
      details: string;
    };
    warning_markdown?: string;
  };
}

interface TableOption {
  label: JSX.Element;
  text: string;
  value: string;
}

const TableOption = ({ table }: { table: Table }) => {
  const { label, type, extra } = table;
  return (
    <TableLabel title={label}>
      {type === 'view' ? (
        <Icons.Eye iconSize="m" />
      ) : (
        <Icons.Table iconSize="m" />
      )}
      {extra?.certification && (
        <CertifiedBadge
          certifiedBy={extra.certification.certified_by}
          details={extra.certification.details}
          size="l"
        />
      )}
      {extra?.warning_markdown && (
        <WarningIconWithTooltip
          warningMarkdown={extra.warning_markdown}
          size="l"
        />
      )}
      {label}
    </TableLabel>
  );
};

const TableSelector: FunctionComponent<TableSelectorProps> = ({
  database,
  formMode = false,
  getDbList,
  handleError,
  isDatabaseSelectEnabled = true,
  onDbChange,
  onSchemaChange,
  onSchemasLoad,
  onTableChange,
  onTablesLoad,
  readOnly = false,
  schema,
  sqlLabMode = true,
  tableName,
}) => {
  const [currentDatabase, setCurrentDatabase] = useState<
    DatabaseObject | undefined
  >(database);
  const [currentSchema, setCurrentSchema] = useState<string | undefined>(
    schema,
  );
  const [currentTable, setCurrentTable] = useState<TableOption | undefined>();
  const [refresh, setRefresh] = useState(0);
  const [previousRefresh, setPreviousRefresh] = useState(0);
  const [loadingTables, setLoadingTables] = useState(false);
  const [tableOptions, setTableOptions] = useState<TableOption[]>([]);
  const { addSuccessToast } = useToasts();


  useEffect(() => {
    if (currentDatabase && currentSchema) {
      setLoadingTables(true);
      const forceRefresh = refresh !== previousRefresh;
      if (previousRefresh !== refresh) {
        setPreviousRefresh(refresh);
      }

        const schemaTablesData = JSON.parse(sessionStorage.getItem("schemaTablesData"));        
          const options: TableOption[] = [];
          let currentTable;
          schemaTablesData.forEach((table: Table) => {
            const option = {
              value: table.value,
              label: <TableOption table={table} />,
              text: table.label,
            };
            options.push(option);
            if (table.label === tableName) {
              currentTable = option;
            }
          });
          if (onTablesLoad) {
            onTablesLoad(schemaTablesData);
          }
          setTableOptions(options);
          setCurrentTable(currentTable);
          setLoadingTables(false);
          if (forceRefresh) addSuccessToast('List updated');         
    }
    // We are using the refresh state to re-trigger the query
    // previousRefresh should be out of dependencies array
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentDatabase, currentSchema, onTablesLoad, refresh]);

  const expandCollapse = () => {
    var coll = document.getElementsByClassName("collapsible");
    var i;

    for (i = 0; i < coll.length; i++) {
      coll[i].addEventListener("click", function () {
        this.classList.toggle("activeTable");
        var content = this.nextElementSibling;
        if (content.style.display === "block") {
          content.style.display = "none";
        } else {
          content.style.display = "block";
        }
      });
    }
  }
  function renderSelectRow(select: ReactNode, refreshBtn: ReactNode) {
    return (
      <div className="section">
        <span className="select tableColumn">{select}</span>
      </div>
    );
  }
  const removeDisabled = () => {
    let element = document.getElementById("arrowIcon1");
   element.classList.remove("fa-disabled");
   let element2 = document.getElementById("arrowIcon2");
   element2.classList.remove("fa-disabled");
   let element3 = document.getElementById("arrowIcon3");
   element3.classList.remove("fa-disabled");
  }

  const internalTableChange = (evt: any, table?: TableOption) => {
    setCurrentTable(table);
    if (onTableChange && currentSchema) {
      onTableChange(table?.value, currentSchema);
    }
    arrowCheck();
    var checkboxes = document.getElementsByName(`${table?.text}`);
    if (evt.target.checked) {
      for (var i = 0; i < checkboxes.length; i++) {
        checkboxes[i].checked = true;
        const item = checkboxes[i];
        var newTable = {
          table: table?.text,
          columns: item.value,
          type: item.dataset.type
        }
        sessionStorage.setItem('arrowClicked', 'false');
        const findItem = tableItemArray.findIndex(e => e?.columns === item.value && e?.table === table?.text)
        if (findItem === -1) {
          tableItemArray.push(newTable);
        }
      }
    } else {
      for (var i = 0; i < checkboxes.length; i++) {
        checkboxes[i].checked = false;
        const item = checkboxes[i];
        const findItem = tableItemArray.findIndex(e => e?.columns === item.value && e?.table === table?.text)
        if (findItem !== -1) {
          tableItemArray.splice(findItem, 1);
        }
      }
      
    }
    sessionStorage.setItem("selectedTableData", JSON.stringify(tableItemArray));
    removeDisabled();
  }; 
  const arrowCheck = () => {
    let arrowCheck = sessionStorage.getItem('arrowClicked');
      if (arrowCheck === 'true') {
        tableItemArray.length = 0;
      }
  } 
  const tableSelection = (evt: any, index: number, table?: TableOption) => {
    var totalCheckbox = document.querySelectorAll(`input[name=${CSS.escape(table.text)}]`).length;
    var totalChecked = document.querySelectorAll(`input[name=${CSS.escape(table.text)}]:checked`).length;

    // When total options equals to total checked option
    if (totalCheckbox == totalChecked) {
      document.getElementsByName("showhide")[index].checked = true;
      
    } else {
      document.getElementsByName("showhide")[index].checked = false;
    }
    // Data session storage
    if (evt.target.checked === true) {
      arrowCheck();
      var newTable = {
        table: table?.text,
        columns: evt.target.value,
        type: evt.target.dataset.type
      }
      sessionStorage.setItem('arrowClicked', 'false');
      tableItemArray.push(newTable);
    } else {
      const findItem = tableItemArray.findIndex(e => e?.columns === evt.target.value)
      if (findItem !== -1) {
        tableItemArray.splice(findItem, 1);
      }
    }
    removeDisabled();
    sessionStorage.setItem("selectedTableData", JSON.stringify(tableItemArray));
  }
  const internalDbChange = (db: DatabaseObject) => {
    setCurrentDatabase(db);
    if (onDbChange) {
      onDbChange(db);
    }
  };

  const internalSchemaChange = (schema?: string) => {
    setCurrentSchema(schema);
    if (onSchemaChange) {
      onSchemaChange(schema);      
      $(".ant-select-dropdown:not([class*='ant-select-dropdown-hidden'])").addClass("ant-select-dropdown-hidden");
    }
  };

  function renderDatabaseSelector() {
    return (
      <DatabaseSelector
        key={currentDatabase?.id}
        db={currentDatabase}
        formMode={formMode}
        getDbList={getDbList}
        handleError={handleError}
        onDbChange={readOnly ? undefined : internalDbChange}
        onSchemaChange={readOnly ? undefined : internalSchemaChange}
        onSchemasLoad={onSchemasLoad}
        schema={currentSchema}
        sqlLabMode={sqlLabMode}
        isDatabaseSelectEnabled={isDatabaseSelectEnabled && !readOnly}
        readOnly={readOnly}
      />
    );
  }

  const handleFilterOption = useMemo(
    () => (search: string, option: TableOption) => {
      const searchValue = search.trim().toLowerCase();
      const { text } = option;
      return text.toLowerCase().includes(searchValue);
    },
    [],
  );

  function renderTableSelect() {
    const disabled =
      (currentSchema && !formMode && readOnly) ||
      (!currentSchema && !database?.allow_multi_schema_metadata_fetch);

    const header = sqlLabMode ? (
      <FormLabel>{t('See table schema')}</FormLabel>
    ) : (
      <FormLabel>{t('Table')}</FormLabel>
    );

    const select = (
      <Accordion allowZeroExpanded allowMultipleExpanded>
        {tableOptions.map((item, index) => {
          return (
            <AccordionItem>
              <AccordionItemHeading>
                <AccordionItemButton>
                  <input
                    type="checkbox"
                    className='leftCheckBox'
                    id={`custom-checkbox-${index}`}
                    name="showhide"
                    value={item.value}
                    onChange={() => internalTableChange(event, item)}
                  />
                  <label htmlFor={`custom-checkbox-${index}`}>{item.text}</label>
                </AccordionItemButton>
              </AccordionItemHeading>
              <AccordionItemPanel>
                {item.label.props.table.columns.map((colData: any, colIndex: any) => {
                  return (
                    <div className="tableSection">
                      <input
                        type="checkbox"
                        className='leftCheckBox'
                        id={`column-checkbox-${colData.name}`}
                        name={item.value}
                        data-type={colData.type}
                        value={`${colData.name}`}
                        onChange={() => tableSelection(event, index, tableOptions[index])}
                      />
                      <label htmlFor={`column-checkbox-${colData.name}`}>{colData.name}</label>
                    </div>
                  )
                })}
              </AccordionItemPanel>
            </AccordionItem>
          )
        })}
      </Accordion>
    );

    const refreshLabel = !formMode && !readOnly && (
      <RefreshLabel
        onClick={() => setRefresh(refresh + 1)}
        tooltipContent={t('Force refresh table list')}
      />
    );

    return renderSelectRow(select, refreshLabel);
  }

  return (
    <TableSelectorWrapper>
      {renderDatabaseSelector()}
      {sqlLabMode && !formMode && <div className="divider" />}
      {renderTableSelect()}
    </TableSelectorWrapper>
  );
};

export default TableSelector;
