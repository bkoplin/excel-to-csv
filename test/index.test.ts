import { expect, it, vi } from 'vitest'
import { select } from '@inquirer/prompts'
import { selectStartingFolder } from '@'

vi.mock('@inquirer/prompts', () => ({
  select: vi.fn(),
  Separator: vi.fn().mockImplementation(text => ({
    type: 'separator',
    line: text,
  })),
}))

const homeFolders = [
  {
    name: 'Desktop',
    value: '/Users/benkoplin/Desktop',
  },
  {
    name: 'Documents',
    value: '/Users/benkoplin/Documents',
  },
  {
    name: 'Downloads',
    value: '/Users/benkoplin/Downloads',
  },
]

const cloudFolders = [
  {
    name: 'SharePoint-MyFiles',
    value: '/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-MyFiles',
  },
]

it('should call select with correct parameters', async () => {
  const expectedChoices = [
    {
      type: 'separator',
      line: '----HOME----',
    },
    ...homeFolders,
    {
      type: 'separator',
      line: '----ONEDRIVE----',
    },
    ...cloudFolders,
  ]

  await selectStartingFolder()

  expect(select).toHaveBeenCalledWith({
    message: 'Where do you want to start looking for your Excel file?',
    pageSize: 20,
    choices: expectedChoices,
  })
})
